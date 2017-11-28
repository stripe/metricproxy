package datadog

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol"

	"golang.org/x/net/context"
)

// Server is the datadog server
type Server struct {
	protocol.CloseableHealthCheck
	listener  net.Listener
	server    http.Server
	collector sfxclient.Collector
	decoder   *decoder
}

// Close the socket currently open for collectd JSON connections
func (s *Server) Close() error {
	return s.listener.Close()
}

// Datapoints returns decoder datapoints
func (s *Server) Datapoints() []*datapoint.Datapoint {
	return append(s.collector.Datapoints(), s.HealthDatapoints()...)
}

var _ protocol.Listener = &Server{}

type DDMetric struct {
	Name       string            `json:"metric"`
	Value      [1][2]interface{} `json:"points"`
	Tags       []string          `json:"tags,omitempty"`
	MetricType string            `json:"type"`
	Hostname   string            `json:"host,omitempty"`
	DeviceName string            `json:"device_name,omitempty"`
	Interval   float64           `json:"interval,omitempty"`
}

type DDMetricsRequest struct {
	Series []DDMetric
}

type decoder struct {
	TotalErrors        int64
	TotalNaNs          int64
	TotalBadDatapoints int64
	SendTo             dpsink.Sink
	Logger             log.Logger
	readAll            func(r io.Reader) ([]byte, error)
	httpClient         *http.Client
}

func (d *decoder) getDatapoints(req *DDMetricsRequest) []*datapoint.Datapoint {
	// dimensions := getDimensions(ts.Labels)
	// metricName := getMetricName(dimensions)
	// if metricName == "" {
	// 	atomic.AddInt64(&d.TotalBadDatapoints, int64(len(ts.Samples)))
	// 	return []*datapoint.Datapoint{}
	// }
	// metricType := getMetricType(metricName)

	dps := make([]*datapoint.Datapoint, 0, len(req.Series))
	for _, s := range req.Series {
		d.Logger.Log(fmt.Sprintf("Got %s", s.Name))
		// ddTS := s.Value[0][0]

		var finalValue float64
		value := s.Value[0][1]
		var err error
		switch value.(type) {
		case string:
			finalValue, err = strconv.ParseFloat(value.(string), 64)
			if err != nil {
				// log.WithField("value", value).Warn("Unable to convert string to float, dropping")
				continue
			}
		case float64:
			finalValue = value.(float64)
		default:
			// log.WithField("type", v).Warn("Unexpected metric value type, dropping")
			continue
		}

		if math.IsNaN(finalValue) {
			atomic.AddInt64(&d.TotalNaNs, 1)
			continue
		}
		sfxValue := datapoint.NewFloatValue(finalValue)

		// TODO use real time from metric
		timestamp := time.Now()
		// TODO Pick a real type, conver the Datadog bits to it, actual do tags, etc
		dps = append(dps, datapoint.New(s.Name, map[string]string{}, sfxValue, datapoint.Counter, timestamp))
	}
	return dps
}

func decodeBody(ctx context.Context, w http.ResponseWriter, r *http.Request, buff io.Writer) (io.ReadCloser, error) {
	var (
		body     io.ReadCloser
		err      error
		encoding = r.Header.Get("Content-Encoding")
	)

	switch encoding {
	case "":
		body = r.Body
		encoding = "identity"
	case "deflate":
		body, err = zlib.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, err
		}
		defer body.Close()
	default:
		http.Error(w, encoding, http.StatusUnsupportedMediaType)
		return nil, err
	}

	// Copy our body so we can pass it on to datadog, then wrap it in a
	// NopCloser cuz we don't really need to close it.
	return ioutil.NopCloser(io.TeeReader(body, buff)), nil
}

// ServeHTTPC decodes datapoints for the connection and sends them to the decoder's sink
func (d *decoder) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	d.Logger.Log(req.URL.Path)
	var err error
	// var compressed []byte
	defer func() {
		if err != nil {
			atomic.AddInt64(&d.TotalErrors, 1)
			log.IfErr(d.Logger, err)
		}
	}()

	var ddMetrics DDMetricsRequest

	var forDatadogBuff bytes.Buffer
	body, err := decodeBody(ctx, rw, req, &forDatadogBuff)
	if err != nil {
		return
	}

	if err = json.NewDecoder(body).Decode(&ddMetrics); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		// log.WithError(err).Error("Could not decode /v1/series request")
		// stats.Count("import.request_error_total", 1, []string{"cause:json"}, 1.0)
	}

	if len(ddMetrics.Series) == 0 {
		const msg = "Received empty /v1/series request"
		http.Error(rw, msg, http.StatusBadRequest)
		// span.Error(errors.New(msg))
		// log.WithError(err).Error(msg)
		return
	}

	dps := d.getDatapoints(&ddMetrics)
	d.Logger.Log(fmt.Sprintf("%+v", dps))

	if len(dps) > 0 {
		err = d.SendTo.AddDatapoints(ctx, dps)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	rw.WriteHeader(http.StatusAccepted)

	endpoint := fmt.Sprintf("https://app.datadoghq.com%s", req.URL.RequestURI())
	SendToDatadog(d.httpClient, endpoint, forDatadogBuff)
}

// Datapoints about this decoder, including how many datapoints it decoded
func (d *decoder) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("datadog.invalid_requests", nil, atomic.LoadInt64(&d.TotalErrors)),
		sfxclient.Cumulative("datadog.total_NAN_samples", nil, atomic.LoadInt64(&d.TotalNaNs)),
		sfxclient.Cumulative("datadog.total_bad_datapoints", nil, atomic.LoadInt64(&d.TotalBadDatapoints)),
	}
}

// Config controls optional parameters for collectd listeners
type Config struct {
	ListenAddr      *string
	Timeout         *time.Duration
	StartingContext context.Context
	HealthCheck     *string
	HTTPChain       web.NextConstructor
	Logger          log.Logger
}

var defaultConfig = &Config{
	ListenAddr:      pointer.String("127.0.0.1:1234"),
	Timeout:         pointer.Duration(time.Second * 30),
	HealthCheck:     pointer.String("/healthz"),
	Logger:          log.Discard,
	StartingContext: context.Background(),
}

func NewListener(sink dpsink.Sink, passedConf *Config) (*Server, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultConfig).(*Config)

	listener, err := net.Listen("tcp", *conf.ListenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()
	metricTracking := &web.RequestCounter{}
	fullHandler := web.NewHandler(conf.StartingContext, web.FromHTTP(r))
	if conf.HTTPChain != nil {
		fullHandler.Add(web.NextHTTP(metricTracking.ServeHTTP))
		fullHandler.Add(conf.HTTPChain)
	}
	decoder := decoder{
		SendTo:     sink,
		Logger:     conf.Logger,
		readAll:    ioutil.ReadAll,
		httpClient: &http.Client{},
	}
	listenServer := Server{
		listener: listener,
		server: http.Server{
			Handler:      fullHandler,
			Addr:         listener.Addr().String(),
			ReadTimeout:  *conf.Timeout,
			WriteTimeout: *conf.Timeout,
		},
		decoder: &decoder,
		collector: sfxclient.NewMultiCollector(
			metricTracking,
			&decoder,
		),
	}
	listenServer.SetupHealthCheck(conf.HealthCheck, r, conf.Logger)
	httpHandler := web.NewHandler(conf.StartingContext, listenServer.decoder)
	SetupDatadogPaths(r, httpHandler)

	go func() {
		log.IfErr(conf.Logger, listenServer.server.Serve(listener))
	}()
	return &listenServer, nil
}

// SetupDatadogPaths tells the router which paths the given handler should handle
func SetupDatadogPaths(r *mux.Router, handler http.Handler) {
	r.Path("/api/v1/series/").Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
}

func SendToDatadog(httpClient *http.Client, endpoint string, body bytes.Buffer) {
	var bodyBuffer bytes.Buffer

	compressor := zlib.NewWriter(&bodyBuffer)
	compressor.Write(body.Bytes())
	compressor.Close() // TODO Check return

	req, err := http.NewRequest(http.MethodPost, endpoint, &bodyBuffer)
	if err != nil {
		fmt.Printf("TODO A THING %v\n", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "deflate")
	// TODO Keepalive?
	req.Close = true

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf("TODO A THING %v\n", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		fmt.Printf("BAD STATUS TODO %d\n", resp.StatusCode)
	}
}

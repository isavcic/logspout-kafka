package kafka

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
	sarama "gopkg.in/Shopify/sarama.v1"
)

func init() {
	router.AdapterFactories.Register(NewKafkaAdapter, "kafka")
}

type KafkaAdapter struct {
	route    *router.Route
	brokers  []string
	topic    string
	producer sarama.AsyncProducer
	tmpl     *template.Template
}

func GenerateTemplate(c *docker.Container) *template.Template {
	var err error
	var tmpl *template.Template

	var custom_text string
	var template_text string

	// pick up custom $NODE_NAME, if it exists
	if os.Getenv("NODE_NAME") != "" {
		custom_text = fmt.Sprintf("node_name=\"%s\"", os.Getenv("NODE_NAME"))
	}

	if os.Getenv("KAFKA_TEMPLATE") != "" {
		template_text = os.Getenv("KAFKA_TEMPLATE")
	}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "K8S_") {
			kv := strings.Split(strings.TrimPrefix(e, "K8S_"), "=")
			// k, v := kv[0], kv[1]
			custom_text = fmt.Sprintf("%s %s=\"%s\"", custom_text, kv[0], kv[1])
		}
	}

	if custom_text != "" || template_text != "" {
		tmpl, err = template.New("kafka").Parse(fmt.Sprintf("%s %s", template_text, custom_text))
		if err != nil {
			errorf("Couldn't parse Kafka message template. %v", err)
		}
	}

	return tmpl
}

func NewKafkaAdapter(route *router.Route) (router.LogAdapter, error) {
	var tmpl *template.Template

	brokers := readBrokers(route.Address)
	if len(brokers) == 0 {
		return nil, errorf("The Kafka broker host:port is missing. Did you specify it as a route address?")
	}

	topic := readTopic(route.Address, route.Options)
	if topic == "" {
		return nil, errorf("The Kafka topic is missing. Did you specify it as a route option?")
	}

	var err error
	// var tmpl *template.Template

	// var node_name_text string
	// var template_text string

	// // pick up custom $NODE_NAME, if it exists
	// if os.Getenv("NODE_NAME") != "" {
	// 	node_name_text = fmt.Sprintf("node_name=\"%s\"", os.Getenv("NODE_NAME"))
	// }

	// if os.Getenv("KAFKA_TEMPLATE") != "" {
	// 	template_text = os.Getenv("KAFKA_TEMPLATE")
	// }

	// if node_name_text != "" || template_text != "" {
	// 	tmpl, err = template.New("kafka").Parse(fmt.Sprintf("%s %s", template_text, node_name_text))
	// 	if err != nil {
	// 		return nil, errorf("Couldn't parse Kafka message template. %v", err)
	// 	}
	// }

	if os.Getenv("DEBUG") != "" {
		log.Printf("Starting Kafka producer for address: %s, topic: %s.\n", brokers, topic)
	}

	var retries int
	retries, err = strconv.Atoi(os.Getenv("KAFKA_CONNECT_RETRIES"))
	if err != nil {
		retries = 3
	}
	var producer sarama.AsyncProducer
	for i := 0; i < retries; i++ {
		producer, err = sarama.NewAsyncProducer(brokers, newConfig())
		if err != nil {
			if os.Getenv("DEBUG") != "" {
				log.Println("Couldn't create Kafka producer. Retrying...", err)
			}
			if i == retries-1 {
				return nil, errorf("Couldn't create Kafka producer. %v", err)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	return &KafkaAdapter{
		route:    route,
		brokers:  brokers,
		topic:    topic,
		producer: producer,
		tmpl:     tmpl,
	}, nil
}

func (a *KafkaAdapter) Stream(logstream chan *router.Message) {
	defer a.producer.Close()
	for rm := range logstream {
		// TODO here we actually pass rm.Container to GenerateTemplate so it can extract
		// env variables from a container
		//
		// a.tmpl = GenerateTemplate(rm.Container)
		message, err := a.formatMessage(rm)
		if err != nil {
			log.Println("kafka:", err)
			a.route.Close()
			break
		}

		a.producer.Input() <- message
	}
}

func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = "logspout"
	config.Producer.Return.Errors = false
	config.Producer.Return.Successes = false
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.RequiredAcks = sarama.WaitForLocal

	if opt := os.Getenv("KAFKA_COMPRESSION_CODEC"); opt != "" {
		switch opt {
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		}
	}

	return config
}

func (a *KafkaAdapter) formatMessage(message *router.Message) (*sarama.ProducerMessage, error) {
	var encoder sarama.Encoder
	if a.tmpl != nil {
		var w bytes.Buffer
		// TODO This is not the best for the template to be executed from, because it is static this way.
		// We need the fields of the template to be dynamic reflecting the env of the container.
		if err := a.tmpl.Execute(&w, message); err != nil {
			return nil, err
		}
		encoder = sarama.ByteEncoder(w.Bytes())
	} else {
		encoder = sarama.StringEncoder(message.Data)
	}

	return &sarama.ProducerMessage{
		Topic: a.topic,
		Value: encoder,
	}, nil
}

func readBrokers(address string) []string {
	if strings.Contains(address, "/") {
		slash := strings.Index(address, "/")
		address = address[:slash]
	}

	return strings.Split(address, ",")
}

func readTopic(address string, options map[string]string) string {
	var topic string
	if !strings.Contains(address, "/") {
		topic = options["topic"]
	} else {
		slash := strings.Index(address, "/")
		topic = address[slash+1:]
	}

	return topic
}

func errorf(format string, a ...interface{}) (err error) {
	err = fmt.Errorf(format, a...)
	if os.Getenv("DEBUG") != "" {
		fmt.Println(err.Error())
	}
	return
}

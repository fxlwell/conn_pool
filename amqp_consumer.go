package connector

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	cook_pool "gitlab.niceprivate.com/golang/cook/pool"
	"time"
)

type AMQPConsumer struct {
	NodeName     string
	MsgHandler   func(msg string) bool
	Exchange     string
	ExchangeType string
	QueueName    string
	WorkNum      int
	WithDeclare  bool
}

const (
	ExTypeFanout  = "fanout"
	ExTypeDirect  = "direct"
	ExTypeTopic   = "topic"
	ExTypeHeaders = "headers"
)

//all channels use one connection
func NewAmqpConsumer(node string, queue string, msg_handler func(msg string) bool, worker int) *AMQPConsumer {
	ac := &AMQPConsumer{
		NodeName:     node,
		MsgHandler:   msg_handler,
		Exchange:     "",
		ExchangeType: ExTypeFanout,
		QueueName:    queue,
		WorkNum:      worker,
		WithDeclare:  false,
	}
	return ac
}

func (ac *AMQPConsumer) SetExchange(exname string) *AMQPConsumer {
	ac.Exchange = exname
	return ac
}

func (ac *AMQPConsumer) Declare(declare bool) *AMQPConsumer {
	ac.WithDeclare = declare
	return ac
}

func (ac *AMQPConsumer) SetExchangeType(extype string) *AMQPConsumer {
	ac.ExchangeType = extype
	return ac
}

func (ac *AMQPConsumer) Run() error {
	if err := ac.connect_handle_with_timeout(5); err != nil {
		return err
	}
	return nil
}

func (ac *AMQPConsumer) connect_handle_with_timeout(timeout int) error {
	r := make(chan error)
	go ac.connect_handle(r)
	select {
	case err := <-r:
		if err != nil {
			return err
		}
	case <-time.After(time.Duration(timeout) * time.Second):
		return fmt.Errorf("amqp logic timeout :%d second ", timeout)
	}
	return nil
}

func (ac *AMQPConsumer) connect_handle(back chan error) {
	conn, err := Get_amqp_obj(ac.NodeName)
	if err != nil {
		back <- err
		return
	}

	for i := 0; i < ac.WorkNum; i++ {
		err := make(chan error)
		go ac.channel_process(conn, err)
		select {
		case info := <-err:
			if info != nil {
				back <- info
				return
			}
		}
	}

	//finally register NotifyClose
	go func() {
		<-conn.NotifyClose(make(chan *amqp.Error))
		for {
			conn.Destroy() //clear conntion pool
			if err := ac.connect_handle_with_timeout(5); err == nil {
				return
			}
			time.Sleep(time.Second * 10)
		}
	}()

	back <- nil
}

func (ac *AMQPConsumer) channel_process(connection *cook_pool.Pool_amqp_conn_obj, back chan error) {
	channel, err := connection.Channel()
	if err != nil {
		back <- err
		return
	}

	q_name := ac.QueueName

	if ac.WithDeclare {
		if err := channel.ExchangeDeclare(
			ac.Exchange,     // name
			ac.ExchangeType, // type
			true,            // durable
			false,           // auto-deleted
			false,           // internal
			false,           // no-wait
			nil,             // arguments
		); err != nil {
			back <- errors.New(fmt.Sprintf("ExchangeDeclare %s", err))
			return
		}

		q, err := channel.QueueDeclare(
			ac.QueueName, // name
			false,        // durable
			false,        // delete when usused
			true,         // exclusive
			false,        // no-wait
			nil,          // arguments
		)
		if err != nil {
			back <- errors.New(fmt.Sprintf("QueueDeclare %s", err))
			return
		}

		if err := channel.QueueBind(
			q.Name,      // queue name
			"",          // routing key
			ac.Exchange, // exchange
			false,
			nil); err != nil {
			back <- errors.New(fmt.Sprintf("QueueBind %s", err))
			return
		}
		q_name = q.Name
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		back <- err
		return
	}

	delivery, err := channel.Consume(q_name, "", false, false, false, false, nil)
	if err != nil {
		back <- err
		return
	}
	back <- nil
	for {
		select {
		case msg, ok := <-delivery:
			if ok {
				ac.msg_handle_timeOut(msg, 1000)
			} else {
				return
			}
		}
	}
}

func (ac *AMQPConsumer) msg_handle_timeOut(msg amqp.Delivery, ms int) error {
	status := make(chan bool)
	allreadyack := false
	go func() {
		if ac.MsgHandler(string(msg.Body)) {
			if !allreadyack {
				msg.Ack(true)
				allreadyack = true
			}
		} else {
			if !allreadyack {
				msg.Nack(true, false)
				allreadyack = true
			}
		}
		status <- true
	}()

	select {
	case <-status:
		return nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		if !allreadyack {
			allreadyack = true
			msg.Ack(true)
		}
		return fmt.Errorf("msg handle timeout :%d ms ", ms)
	}
	return nil
}

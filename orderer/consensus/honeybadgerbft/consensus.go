/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
                 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package honeybadgerbft

import (
	"fmt"
	"sync"
	"time"

	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"

	"encoding/binary"
	"io"
	"net"

	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = logging.MustGetLogger("orderer/honeybadgerbft")
var sendSocketPath = ""
var receiveSocketPath = ""

//measurements
var interval = int64(10000)
var envelopeMeasurementStartTime = int64(-1)
var countEnvelopes = int64(0)

type consenter struct{}

type chain struct {
	support           consensus.ConsenterSupport
	sendChan          chan *cb.Envelope
	exitChan          chan struct{}
	sendConnection    net.Conn
	receiveConnection net.Listener
	sendLock          *sync.Mutex
}

// New creates a new consenter for the HoneyBadgerBFT consensus scheme.
// It communicates with a HoneyBadgerBFT node via Unix websockets and simply marshals/sends and receives/unmarshals
// messages.
func New(config localconfig.HoneyBadgerBFT) consensus.Consenter {
	sendSocketPath = config.SendSocketPath
	receiveSocketPath = config.ReceiveSocketPath
	return &consenter{}
}

func (consenter *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support), nil
}

func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support:  support,
		sendChan: make(chan *cb.Envelope),
		exitChan: make(chan struct{}),
		sendLock: &sync.Mutex{},
	}
}

func (ch *chain) Start() {
	conn, err := net.Dial("unix", sendSocketPath)

	if err != nil {
		logger.Errorf("Could not connect to send proxy on path %s!", sendSocketPath)
		logger.Error(err)
		return
	} else {
		logger.Infof("Connected to send proxy!")
	}

	ch.sendConnection = conn

	listen, err := net.Listen("unix", receiveSocketPath)

	if err != nil {
		logger.Errorf("Could not connect to receive proxy on path %s!", receiveSocketPath)
		logger.Error(err)
		return
	} else {
		logger.Infof("Connected to receive proxy!")
	}

	ch.receiveConnection = listen

	go ch.connLoop()

	go ch.appendToChain()
}

func (ch *chain) Halt() {

	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

// Configure accepts configuration update messages for ordering
// TODO
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	//select {
	//case ch.sendChan <- &message{
	//	configSeq: configSeq,
	//	configMsg: config,
	//}:
	//	return nil
	//case <-ch.exitChan:
	//	return fmt.Errorf("Exiting")
	//}

	return nil;
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *chain) sendLength(length int, conn net.Conn) (int, error) {
	var buf [8]byte

	logger.Infof("Sending length to proxy: %s", length)

	binary.BigEndian.PutUint64(buf[:], uint64(length))

	return conn.Write(buf[:])
}

func (ch *chain) sendEnvToBFTProxy(env *cb.Envelope) (int, error) {
	ch.sendLock.Lock()
	bytes, err := utils.Marshal(env)

	if err != nil {
		return -1, err
	}

	status, err := ch.sendLength(len(bytes), ch.sendConnection)

	if err != nil {
		return status, err
	}

	logger.Infof("Sending bytes to proxy: %s", bytes)

	i, err := ch.sendConnection.Write(bytes)

	ch.sendLock.Unlock()

	return i, err
}

func (ch *chain) recvLength(conn net.Conn) (int64, error) {
	var size int64
	err := binary.Read(conn, binary.BigEndian, &size)

	logger.Infof("Receiving length to proxy: %s", size)

	return size, err
}

func (ch *chain) recvBytes(conn net.Conn) ([]byte, error) {
	size, err := ch.recvLength(conn)

	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)

	_, err = io.ReadFull(conn, buf)

	if err != nil {
		return nil, err
	}

	logger.Infof("Receiving bytes from proxy: %s", buf)

	return buf, nil
}

func (ch *chain) recvEnvFromBFTProxy(conn net.Conn) (*cb.Envelope, error) {
	size, err := ch.recvLength(conn)

	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)

	_, err = io.ReadFull(conn, buf)

	logger.Infof("Receiving env bytes from proxy: %s", buf)

	if err != nil {
		return nil, err
	}

	env, err := utils.UnmarshalEnvelope(buf)

	if err != nil {
		return nil, err
	}

	return env, nil
}

// Order accepts a message and returns true on acceptance, or false on shutdown
func (ch *chain) Order(env *cb.Envelope, _ uint64) error {

	_, err := ch.sendEnvToBFTProxy(env)

	if err != nil {
		return err
	}

	if envelopeMeasurementStartTime == -1 {
		envelopeMeasurementStartTime = time.Now().UnixNano()
	}

	countEnvelopes++
	if countEnvelopes%interval == 0 {

		tp := float64(interval*1000000000) / float64(time.Now().UnixNano()-envelopeMeasurementStartTime)
		fmt.Printf("Throughput = %v envelopes/sec\n", tp)
		envelopeMeasurementStartTime = time.Now().UnixNano()

	}

	select {
	case <-ch.exitChan:
		return fmt.Errorf("exiting")
	default:
		return nil
	}
}

func (ch *chain) connLoop() {
	for {
		conn, err := ch.receiveConnection.Accept()
		if err != nil {
			logger.Errorf("[recv] Error while accepting connection from HoneyBadgerBFT proxy: %v\n", err)
			continue
		}

		block, err := ch.recvEnvFromBFTProxy(conn)
		if err != nil {
			logger.Errorf("[recv] Error while receiving envelope from HoneyBadgerBFT proxy: %v\n", err)
			continue
		}

		ch.sendChan <- block
	}
}

func (ch *chain) appendToChain() {
	var timer <-chan time.Time
	var err error

	for {
		err = nil
		select {
		case msg := <-ch.sendChan:
			// NormalMsg
			_, err = ch.support.ProcessNormalMsg(msg)
			if err != nil {
				logger.Warningf("Discarding bad normal message: %s", err)
				continue
			}
			batches, _ := ch.support.BlockCutter().Ordered(msg)
			if len(batches) == 0 && timer == nil {
				timer = time.After(ch.support.SharedConfig().BatchTimeout())
				continue
			}
			for _, batch := range batches {
				block := ch.support.CreateNextBlock(batch)
				ch.support.WriteBlock(block, nil)
			}
			if len(batches) > 0 {
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			batch := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, nil)
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}

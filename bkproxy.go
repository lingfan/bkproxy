package main

import (
	"io"
	jconfig "github.com/stathat/jconfig"
	log "github.com/moovweb/log4go"
	"net"
	"os"
)

const (
	RECV_BUF_LEN = 1024
)

const (
	ST_CL_DISCON = iota
	ST_CL_EOF
	ST_CL_ERR
	ST_BE_EOF
	ST_BE_ERR
)
func main() {
	config := jconfig.LoadConfig("conf.json")

	//confBE := config.GetArray("backend")
	backendPool := make([]*Backend, 10)
	for i, v := range config.GetArray("backend") {
		log.Info("backend %d = %s", i, v)
		be, _ := NewBackend(v.(string))
		backendPool[i] = be
	}

	listenStr := config.GetString("listen_host") + ":"+ config.GetString("listen_port")
	listenAddr, err := net.ResolveTCPAddr("tcp", listenStr)
	if err != nil {
		log.Error(err)
	}
	log.Info("Starting the server on %s", listenAddr)
	listener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		log.Error("error listening:%s", err.Error())
		log.Close()
		os.Exit(1)
	}

	bc := 0 //backend counter
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Error("Error accept:%s", err.Error())
			log.Close()
			os.Exit(1)
		}
		go ProxyFunc(conn, backendPool[bc])
		bc = (bc + 1) % len(backendPool)
	}
}

func ProxyFunc(conn *net.TCPConn, be *Backend) {
	//connect to backend
	bconn, err := be.NewConn()
	if err != nil {
		log.Error("can't create new backend:", err.Error())
	}

	//back forwarder
	ch_bf_data := make(chan []byte)
	ch_bf_status := make(chan int)
	go backForwarder(bconn, ch_bf_data, ch_bf_status)

	//back receiver
	ch_br_data := make(chan []byte)
	ch_br_st := make(chan int)
	go backRecvr(bconn, ch_br_data, ch_br_st)

	//client forwarder
	ch_cf_data := make(chan []byte)
	ch_cf_status := make(chan int)
	go clientForwarder(conn, ch_cf_data, ch_cf_status)

	//client receiver
	ch_cr_data := make(chan []byte)
	ch_cr_status := make(chan int)
	go clientRecvr(conn, ch_cr_data, ch_cr_status)

	for {
		select {
		case cr_data :=<-ch_cr_data://some data from client, forward to backend
			ch_bf_data <- cr_data
		case br_data:=<-ch_br_data://some data from backend, forward to client
			ch_cf_data<-br_data
		case _ =<-ch_cr_status:
			println("unhandled : status dari cr")
			os.Exit(1)
		case _ = <-ch_bf_status:
			println("unhandled : status dari bf")
			os.Exit(1)
		case br_st :=<-ch_br_st:
			if br_st == ST_BE_EOF {
				println("unhandled : status dari br = EOF")
			}
			println("unhandled : status dari br")
			os.Exit(1)
		case _ = <-ch_cf_status:
			println("unhandled : status dari cf")
			os.Exit(1)
		}
	}
}

//forward all packet to client
func clientForwarder(conn *net.TCPConn, ch_data chan []byte, ch_status chan int) {
	for {
		data := <-ch_data
		n, err := conn.Write(data)
		if err != nil {
			log.Error(err)
			ch_status <- ST_BE_ERR
			return
		}
		if n != len(data) {
			ch_status <- ST_BE_ERR
			return
		}
	}
}

//Receive all packet from backend
func backRecvr(conn *net.TCPConn, ch_data chan []byte, ch_status chan int) {
	for {
		buf := make([]byte, RECV_BUF_LEN)
		n, err := conn.Read(buf)
		if err != nil {
			if !isNetTempErr(err) {
				if err == io.EOF {
					ch_status <- ST_BE_EOF
					println("backend err = EOF")
					return
				}
				println("backend err = UNKNOWN")
				ch_status <- ST_BE_ERR
				return
			}
		}
		println("beRecv : recv :", n)
		ch_data <- buf
	}
}

//forward all packet to backend
func backForwarder(bconn *net.TCPConn, ch_data chan []byte, ch_status chan int) {
	for {
		data := <-ch_data
		println("BF get data")
		n, err := bconn.Write(data)
		if err != nil {
			log.Error(err)
			ch_status <- ST_BE_ERR
			return
		}
		if n != len(data) {
			ch_status <- ST_BE_ERR
			return
		}
	}
}
//Client Receiver : receive all packet from client
func clientRecvr(conn *net.TCPConn, ch_data chan []byte, ch_status chan int) {
	for {
		buf := make([]byte, RECV_BUF_LEN)
		n, err := conn.Read(buf)
		if err != nil {
			if !isNetTempErr(err) {
				if err == io.EOF {
					ch_status <- ST_CL_EOF
					return
				}
				ch_status <- ST_CL_ERR
				return
			}
		}
		println("cRecv : recv :", n)
		ch_data <- buf
	}
}

func isNetTempErr(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
		return true
	}
	return false
}

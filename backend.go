package main

import (
	log "github.com/moovweb/log4go"
	"net"
)

type Backend struct {
	StrAddr string
	Addr *net.TCPAddr
}

func NewBackend(strAddr string) (*Backend, error) {
	addr, err := net.ResolveTCPAddr("tcp", strAddr)
	if err != nil {
		log.Error("Can't resolve backend addr:", err.Error())
		return nil, err
	}

	be := new(Backend)
	be.StrAddr = strAddr
	be.Addr = addr
	return be, nil
}

func (be *Backend) NewConn() (*net.TCPConn, error) {
	println("backend connectiing to :", be.StrAddr)
	conn, err := net.DialTCP("tcp", nil, be.Addr)
	if err != nil {
		log.Error("NewConn:Dial failed:", err.Error())
		return nil, err
	}
	return conn, nil
}

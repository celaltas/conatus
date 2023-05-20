package main

import (
	"errors"
	"fmt"
	"log"
	"syscall"
)

type State int

const (
	STATE_REQ State = iota
	STATE_RES
	STATE_END
)

type Connection struct {
	fd              int
	state           State
	readBufferSize  uint
	readBuffer      []byte
	writeBufferSize uint
	writeBufferSent uint
	writeBuffer     []byte
}

func NewConnection(fd int) *Connection {

	return &Connection{
		fd:              fd,
		state:           STATE_REQ,
		readBufferSize:  0,
		readBuffer:      make([]byte, MESSAGE_HEADER_LENGTH+MESSAGE_LIMIT),
		writeBufferSize: 0,
		writeBufferSent: 0,
		writeBuffer:     make([]byte, MESSAGE_HEADER_LENGTH+MESSAGE_LIMIT),
	}
}

func ConnectionPut(connections map[int32]*Connection, connection *Connection) {
	connections[int32(connection.fd)] = connection
}

func acceptNewConnection(connections map[int32]*Connection, fd int) error {
	connfd, _, err := syscall.Accept(fd)
	setNonBlocking(connfd)
	if err != nil {
		return fmt.Errorf("error when accept connection: %s", err.Error())
	}
	conn := NewConnection(connfd)
	ConnectionPut(connections, conn)
	return nil

}

func ConnectionIO(conn *Connection) error {
	if conn.state == STATE_REQ {
		StateReq(conn)
	} else if conn.state == STATE_RES {
		StateRes(conn)
	} else {
		return errors.New("error when connection state is unknown")
	}
	return nil

}

func StateReq(conn *Connection) {
	for TryFillBuffer(conn) {
	}
}

func TryFillBuffer(conn *Connection) bool {
	if int(conn.readBufferSize) >= len(conn.readBuffer) {
		log.Fatal("allocated buffer size is smaller than read buffer size")
	}

	cap := len(conn.readBuffer) - int(conn.readBufferSize)
	rv, err := syscall.Read(conn.fd, conn.readBuffer[conn.readBufferSize:cap+int(conn.readBufferSize)])
	for rv < 0 && err == syscall.EINTR {
		rv, err = syscall.Read(conn.fd, conn.readBuffer[conn.readBufferSize:cap+int(conn.readBufferSize)])
	}
	if rv < 0 && err == syscall.EAGAIN {
		return false
	}

	if rv == 0 {
		if conn.readBufferSize > 0 {
			log.Println("error while reading from connection unexpected eof")
		} else {
			log.Println("error while reading from connection eof")
		}
		conn.state = STATE_END
		return false
	}

	conn.readBufferSize += uint(rv)
	if conn.readBufferSize >= uint(len(conn.readBuffer)) {
		log.Fatal("allocated buffer size is smaller than read buffer size")
	}
	for tryOneRequest(conn) {
	}
	return conn.state == STATE_REQ
}

func StateRes(conn *Connection) {
	for TryFlushBuffer(conn) {
	}
}

func TryFlushBuffer(conn *Connection) bool {
	remain := conn.writeBufferSize - conn.writeBufferSent
	rv, err := syscall.Write(conn.fd, conn.writeBuffer[conn.writeBufferSent:remain+conn.writeBufferSent])

	for rv < 0 && err == syscall.EINTR {
		rv, err = syscall.Write(conn.fd, conn.writeBuffer[conn.writeBufferSent:remain+conn.writeBufferSent])
	}

	if rv < 0 && err == syscall.EAGAIN {
		return false
	}

	if rv < 0 {
		conn.state = STATE_END
		return false
	}

	conn.writeBufferSent += uint(rv)
	if conn.writeBufferSent > conn.writeBufferSize {
		log.Fatal("sending data size is smaller than write buffer size")
	}

	if conn.writeBufferSent == conn.writeBufferSize {
		conn.state = STATE_REQ
		conn.writeBufferSent = 0
		conn.writeBufferSize = 0
		return false
	}
	return true

}

func setNonBlocking(fd int) error {
	flags, _, err := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFL, 0)
	if err != 0 {
		return err
	}

	flags |= syscall.O_NONBLOCK

	_, _, err = syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFL, flags)
	if err != 0 {
		return err
	}

	return nil
}

package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net"
)

const (
	SERVER_HOST         = "localhost"
	SERVER_PORT_C       = "9988"
	SERVER_TYPE         = "tcp"
	messageLimit        = 4096
	messageHeaderLength = 4
)

func main() {

	flag.Parse()
	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT_C)
	if err != nil {
		panic(err)
	}

	if err := sendRequest(connection, flag.Args()); err != nil {
		fmt.Println(err)
	}

	if err := readResponse(connection); err != nil {
		fmt.Println(err)
	}

	defer connection.Close()

}

func writeAll(connection net.Conn, buffer []byte, length uint32) error {

	for length > 0 {
		rv, err := connection.Write(buffer[:length])
		if err != nil || rv <= 0 {
			return err
		}
		length -= uint32(rv)
		buffer = buffer[rv:]
	}
	return nil
}

func readFull(connection net.Conn, buffer []byte, length uint32) error {

	for length > 0 {
		rv, err := connection.Read(buffer[:length])
		if err != nil || rv < 0 {
			return err
		}
		if rv == 0 {
			return errors.New("EOF")
		}
		if uint32(rv) > length {
			return errors.New("more than byte read from connection")
		}
		length -= uint32(rv)
		buffer = buffer[rv:]
	}
	return nil

}

func sendRequest(connection net.Conn, cmd []string) error {

	var length uint32 = 4
	for _, s := range cmd {
		length += 4 + uint32(len(s))
	}
	if length > messageLimit {
		return errors.New("message too large")
	}

	buf := make([]byte, messageHeaderLength+messageLimit)
	binary.LittleEndian.PutUint32(buf[:4], length)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(cmd)))

	cur := 8
	for _, s := range cmd {
		p := uint32(len(s))
		binary.LittleEndian.PutUint32(buf[cur:cur+4], p)
		copy(buf[cur+4:cur+4+int(p)], []byte(s))
		cur += 4 + int(p)
	}
	fmt.Println("write buffer:", buf)

	return writeAll(connection, buf, 4+length)

}

func readResponse(conn net.Conn) error {
	rbuf := make([]byte, messageHeaderLength+messageLimit)
	if err := readFull(conn, rbuf, messageHeaderLength); err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(rbuf[:messageHeaderLength])
	if length > messageLimit {
		return errors.New("message too large")
	}
	if err := readFull(conn, rbuf[messageHeaderLength+4:], length); err != nil {
		return err
	}
	rescode := binary.LittleEndian.Uint32(rbuf[4:8])
	if length < 4 {
		return errors.New("bad response")
	}

	fmt.Printf("server says: [%d] %s\n", rescode, rbuf[8:])
	return nil
}

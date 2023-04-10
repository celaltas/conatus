package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	SERVER_HOST         = "localhost"
	SERVER_PORT_C       = "9988"
	SERVER_TYPE         = "tcp"
	messageLimit        = 4096
	messageHeaderLength = 4
)

func main() {

	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT_C)
	if err != nil {
		panic(err)
	}

	query(connection, "Mens sana in corpore sano")
	fmt.Println("first request send")
    time.Sleep(5 * time.Second) 
	query(connection, "Amor vincit omnia")
	fmt.Println("second request send")
    time.Sleep(5 * time.Second) 
	query(connection, "De gustibus non est disputandum")
	fmt.Println("third request send")
	
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

func query(connection net.Conn, message string) error {

	if len(message) > messageLimit {
		return errors.New("message too long")
	}

	writeBuffer := make([]byte, messageHeaderLength+messageLimit)
	binary.LittleEndian.PutUint32(writeBuffer[0:], uint32(len(message)))
	copy(writeBuffer[messageHeaderLength:], []byte(message))
	if err := writeAll(connection, writeBuffer, uint32(messageHeaderLength+len(message))); err != nil {
		return err
	}

	messageLength := make([]byte, messageHeaderLength)
	if err := readFull(connection, messageLength, messageHeaderLength); err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(messageLength)
	if length > messageLimit {
		return errors.New("message too large")
	}
	messageContent := make([]byte, messageLimit)
	err := readFull(connection, messageContent, length)
	if err != nil {
		return errors.New("error when reading message")
	}
	fmt.Printf("Server says: %s\n", string(messageContent))

	return nil

}

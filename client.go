package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

const (
	SERVER_HOST         = "localhost"
	SERVER_PORT         = "9988"
	SERVER_TYPE         = "tcp"
	messageLimit        = 4096
	messageHeaderLength = 4
)

func main() {

	connection, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		panic(err)
	}
	query(connection, "hello1")
	query(connection, "hello2")
	query(connection, "hello3")
	query(connection, "hello4")
	query(connection, "hello5")
	query(connection, "hello6")
	query(connection, "hello7")
	query(connection, "hello8")
	query(connection, "hello9")
	defer connection.Close()

}

func writeAll(connection net.Conn, buffer []byte, length uint32) error {
	_, err := connection.Write(buffer)
	if err != nil {
		return err
	}
	fmt.Println("Message written!!!")
	return nil
}

func readFull(connection net.Conn, buffer []byte, length uint32) error {
	_, err := connection.Read(buffer)
	if err != nil {
		return err
	}
	return nil

}

func query(connection net.Conn, message string) error {

	if len(message) > messageLimit {
		return errors.New("Message too long!")
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
		return errors.New("Message too large")
	}
	messageContent := make([]byte, messageLimit)
	err := readFull(connection, messageContent, length)
	if err != nil {
		return errors.New("Error when reading message!")
	}
	fmt.Printf("Server says: %s\n", string(messageContent))

	return nil

}







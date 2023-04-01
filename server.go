package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"unsafe"
)

const (
	SERVER_HOST         = "localhost"
	SERVER_PORT         = "9988"
	SERVER_TYPE         = "tcp"
	messageLimit        = 4096
	messageHeaderLength = 4
)

func main() {
	server, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	if err != nil {
		panic(err)
	}
	defer server.Close()
	fmt.Println("Listening on " + SERVER_HOST + ":" + SERVER_PORT)
	fmt.Println("Waiting for client...")

	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("Client connected..")

		go func(conn net.Conn) {
			defer conn.Close()
			for {
				if err := oneRequest(conn); err != nil {
					fmt.Println("when request handling:", err)
					break
				}
			}
			fmt.Println("Client disconnected.")
		}(connection)
	}
}



func readFull(connection net.Conn, buffer []byte, length uint32) error {
	_, err := connection.Read(buffer)
	if err != nil {
		return err
	}
	return nil

}

func writeAll(connection net.Conn, buffer []byte, length uint32) error {
	_, err := connection.Write(buffer)
	if err != nil {
		return err
	}
	return nil
}

func oneRequest(connection net.Conn) error {

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
	fmt.Printf("Client says: %s\n", string(messageContent))

	reply := []byte("world")
	writeBuffer := make([]byte, unsafe.Sizeof(reply))

	binary.LittleEndian.PutUint32(writeBuffer[:messageHeaderLength], uint32(len(reply)))
	copy(writeBuffer[messageHeaderLength:], reply)

	return writeAll(connection, writeBuffer, uint32(messageHeaderLength+len(reply)))
}

package main

import (
	"bytes"
	"encoding/json"
	_ "encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
    if err != nil {
        log.Fatalf("%s: %s", msg, err)
    }
}


func AmqpConnection() *amqp091.Connection {
    err := godotenv.Load(); 

    if err != nil {
        log.Fatal("Error loading .env file")
        return nil; 
    }

    user := os.Getenv("userRabbit")
    password := os.Getenv("passwordRabbit")
    ip := os.Getenv("INSTANCE_IP")

    br, err := amqp091.Dial("amqp://" + user + ":" + password + "@" + ip + "/")

    if err != nil {
        log.Panicf("error to get instance!"); 
        return nil; 
    }

    return br
}


func main() {
    conn := AmqpConnection(); 

    if conn == nil {
        log.Panicf("error to get connection!");
    }

    defer conn.Close(); 

    go SendMessageWeight(conn);


    select {};

}

func SendMessageWeight(br *amqp091.Connection) {
    ch, err := br.Channel()
    failOnError(err, "Error al abrir un canal")
    defer ch.Close()

    q, err := ch.QueueDeclare(
        "data", 
        true,      
        false,   
        false,     
        false,      
        nil,        
    )
    failOnError(err, "Error al declarar la cola")

    err = ch.QueueBind(
        q.Name,             
        "peso",        
        "orangesExchange", 
        false,            
        nil,                 
    )
    failOnError(err, "Error al enlazar la cola")

    msgs, err := ch.Consume(
        q.Name,
        "",    
        true,   // auto-ack
        false,  // exclusive
        false,  // no-local
        false,  // no-wait
        nil,    // argumentos
    )
    failOnError(err, "Error al registrar el consumidor")

    forever := make(chan bool)

    go func() {
		for d := range msgs {
			fmt.Printf("message: %s", d.Body); 
		
			var orange *struct{};
            
			err := json.Unmarshal(d.Body, orange)

			if err != nil {
				fmt.Printf("erro to unmarshar data!");
			}

			Json, errSerialize := json.Marshal(orange);

			if errSerialize != nil {
				fmt.Printf("error to serialize message into a json!")
			}
			ryder := bytes.NewReader(Json); 			

			query, errSendMessage := http.Post("http://localhost:8000", "applcation/json", ryder); 

			if errSendMessage != nil {
				fmt.Printf("error to send message!"); 
			}

		
			fmt.Print("message", query)
			}
    }()

    fmt.Println("Esperando mensajes...")
    <-forever
}
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"io"
	"github.com/joho/godotenv"
	"github.com/rabbitmq/amqp091-go"
)

// OrangeInputData representa los datos recibidos del sensor
type OrangeInputData struct {
    Peso    float64 `json:"peso"`
    Tamano  string  `json:"tamano"`
    Color   string  `json:"color"`
    Esp32FK string  `json:"esp32_fk"`
}

// OrangeOutputData representa los datos que enviaremos a la API (mismo formato)
type OrangeOutputData struct {
    Peso    float64 `json:"peso"`
    Tamano  string  `json:"tamano"`
    Color   string  `json:"color"`
    Esp32FK string  `json:"esp32_fk"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func AmqpConnection() *amqp091.Connection {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
		return nil
	}

	user := os.Getenv("userRabbit")
	password := os.Getenv("passwordRabbit")
	ip := os.Getenv("INSTANCE_IP")

	br, err := amqp091.Dial("amqp://" + user + ":" + password + "@" + ip + "/")

	if err != nil {
		log.Panicf("error to get instance!")
		return nil
	}

	return br
}

func main() {
	conn := AmqpConnection()

	if conn == nil {
		log.Panicf("error to get connection!")
	}

	defer conn.Close()

	go SubscribeToOrangeData(conn)

	select {}
}

func SubscribeToOrangeData(br *amqp091.Connection) {
	ch, err := br.Channel()
	failOnError(err, "Error al abrir un canal")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"orange_queue", // Nombre de la cola para las naranjas
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error al declarar la cola")

	err = ch.QueueBind(
		q.Name,
		"test",      // Routing key para los datos de naranjas
		"amq.topic", // Exchange MQTT por defecto en RabbitMQ
		false,
		nil,
	)
	failOnError(err, "Error al enlazar la cola con el topic orange.data")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Error al registrar el consumidor")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			fmt.Printf("Mensaje recibido: %s\n", d.Body)

			// Procesar el mensaje y enviarlo a la API
			processOrangeDataAndSendToAPI(d.Body)
		}
	}()

	fmt.Println("Esperando mensajes en el topic 'test'...")
	<-forever
}

func processOrangeDataAndSendToAPI(data []byte) {
    // Imprimir el JSON recibido para debug
    fmt.Printf("JSON recibido: %s\n", string(data))
    
    var inputData OrangeInputData
    err := json.Unmarshal(data, &inputData)
    if err != nil {
        log.Printf("Error al parsear JSON: %v", err)
        return
    }
    
    // Verificar que todos los campos necesarios estén presentes
    if inputData.Esp32FK == "" {
        log.Printf("Error: Campo esp32_fk vacío o no presente")
        return
    }
    
    // Crear los datos de salida manteniendo el formato exacto
    outputData := OrangeOutputData{
        Peso:    inputData.Peso,
        Tamano:  inputData.Tamano,
        Color:   inputData.Color,
        Esp32FK: inputData.Esp32FK,  // Usar el campo correcto
    }
    
    // Convertir a JSON
    jsonData, err := json.Marshal(outputData)
    if err != nil {
        log.Printf("Error al crear JSON de salida: %v", err)
        return
    }
    
    // Imprimir JSON de salida para verificar
    fmt.Printf("JSON enviado a API: %s\n", string(jsonData))
    
    // Enviar datos a la API
    resp, err := http.Post(
        "http://52.4.21.111:8082/naranjas/",
        "application/json",
        bytes.NewBuffer(jsonData),
    )
    
    if err != nil {
        log.Printf("Error al enviar datos a la API: %v", err)
        return
    }
    defer resp.Body.Close()
    
    // Leer el cuerpo de la respuesta para debug
    var respBody []byte
    respBody, _ = io.ReadAll(resp.Body)
    
    if resp.StatusCode >= 200 && resp.StatusCode < 300 {
        log.Printf("Datos enviados exitosamente a la API. Código: %d", resp.StatusCode)
    } else {
        log.Printf("Error al enviar datos a la API. Código: %d, Respuesta: %s", 
            resp.StatusCode, string(respBody))
    }
}

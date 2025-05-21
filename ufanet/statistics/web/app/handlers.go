package app

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sort"
	. "statistics/models"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/segmentio/kafka-go"
)

var ctxBackground = context.Background()

func GetCategory(c fiber.Ctx) error {
	var category Category
	db.Table("billboard_category").First(&category, c.Params("id"))
	return c.JSON(category)
}

func GetSale(c fiber.Ctx) error {
	var sale Sale
	db.Table("billboard_sale").First(&sale, c.Params("id"))
	return c.JSON(sale)
}

func IndexHandler(c fiber.Ctx) error {
	return c.Render("index", fiber.Map{})
}

func SaleHandler(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(ctxBackground, 5*time.Second)
	defer cancel()

	saleMessages, popularitySalesStruct := getMessagesTopic[Sale]("wal_listener.public_billboard_sale", ctx)

	return c.Render("sale", fiber.Map{
		"saleMessages":          saleMessages,
		"popularitySalesStruct": popularitySalesStruct,
	})
}

func CategoryHandler(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(ctxBackground, 5*time.Second)
	defer cancel()

	categoryMessages, popularityCategoriesStruct := getMessagesTopic[Category]("wal_listener.public_billboard_category", ctx)
	return c.Render("category", fiber.Map{
		"categoryMessages":           categoryMessages,
		"popularityCategoriesStruct": popularityCategoriesStruct,
	})
}

func getMessagesTopic[T SaleCategory](topic string, ctx context.Context) ([]Message[T], []Popularity[T]) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   topic,
	})
	defer reader.Close()

	var messages []Message[T]
	popularity := make(map[T]int)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println(err)
			break
		}

		var message Message[T]
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Println(err)
		}

		if message.Action == "SELECT" {
			popularity[message.Data]++
		}

		messages = append(messages, message)
	}

	var popularityStruct []Popularity[T]
	for key, value := range popularity {
		popularityStruct = append(popularityStruct, Popularity[T]{Struct: key, View: value})
	}

	sort.Slice(popularityStruct, func(i, j int) bool {
		return popularityStruct[i].View < popularityStruct[j].View
	})

	slices.Reverse(popularityStruct)
	return messages, popularityStruct
}

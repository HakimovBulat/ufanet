package app

import (
	"log"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/template/html/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

func RunApp() {
	engine := html.New("../web/templates", ".html")

	dsn := "host=localhost user=postgres password=postgres dbname=postgres port=5432 sslmode=disable"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	app := fiber.New(fiber.Config{
		Views: engine,
	})

	app.Get("/", IndexHandler)
	app.Get("/category", CategoryHandler)
	app.Get("/sale", SaleHandler)

	app.Get("/category/:id", GetCategory)
	app.Get("/sale/:id", GetSale)

	log.Fatal(app.Listen(":3000"))
}

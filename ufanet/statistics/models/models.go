package models

import (
	"context"

	"github.com/gofrs/uuid"
)

type SaleCategory interface {
	Sale | Category
}

type Message[T SaleCategory] struct {
	Data       T         `json:"data"`
	Id         uuid.UUID `json:"id"`
	Schema     string    `json:"schema"`
	Action     string    `json:"action"`
	DataOld    T         `json:"dataOld"`
	CommitTime string    `json:"commitTime"`
	Table      string    `json:"table"`
}

var ctxBackground = context.Background()

type Sale struct {
	Id           int    `json:"id"`
	Title        string `json:"title"`
	Subtitle     string `json:"subtitle"`
	Photo        string `json:"photo"`
	AboutPartner string `json:"about_partner"`
	Promocode    string `json:"promocode"`
	StartDate    string `json:"start_date"`
	EndDate      string `json:"end_date"`
	Url          string `json:"url"`
}

type Category struct {
	Id    int    `json:"id"`
	Title string `json:"title"`
	Photo string `json:"photo"`
}

type Popularity[T SaleCategory] struct {
	Struct T
	View   int
}

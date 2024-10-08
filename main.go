package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dbfixture"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"log"
	"os"
	"reflect"
	"time"
)

type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID        int64     `bun:",pk,autoincrement"`
	Username  string    `bun:",notnull"`
	Email     string    `bun:",notnull,unique"`
	Password  string    `bun:",notnull"`
	CreatedAt time.Time `bun:",nullzero,default:current_timestamp"`
}

type user struct {
	ID        int64
	UpdatedAt time.Time
}

type ProductPart struct {
	ID                string
	CorrelationNumber int
}

type Product struct {
	ID    string
	Price float64
}

func main() {
	dsn := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	db := bun.NewDB(sqldb, pgdialect.New())
	db.RegisterModel((*User)(nil))

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		failingCode(db, ctx)
		passingCode(db, ctx)
	}

	user := &ProductPart{CorrelationNumber: 123, ID: "Lock"}
	product := &Product{}

	MapStructs(product, user)
	fmt.Printf("Mapped product: %+v\n", product)
}

func failingCode(db *bun.DB, ctx context.Context) {
	// Load fixtures
	fixture := dbfixture.New(db, dbfixture.WithTruncateTables())
	err := fixture.Load(ctx, os.DirFS("testdata"), "fixtures.yaml")
	if err != nil {
		log.Fatalf("(failingCode) failed to load fixtures: %v", err)
	}

	fmt.Println("Doe", fixture.MustRow("User.doe").(*User))
}

func passingCode(db *bun.DB, ctx context.Context) {
	var err error
	fixture := dbfixture.New(db, dbfixture.WithTruncateTables())
	err = fixture.Load(ctx, os.DirFS("testdata"), "fixtures.yaml")
	if err != nil {
		log.Fatalf("(passingCode) failed to load fixtures: %v", err)
	}

	var rows []*user

	err = db.NewRaw(
		"SELECT id, updated_at FROM (SELECT u1.id, u1.created_at as updated_at, ROW_NUMBER() over (PARTITION BY u1.id) as row_num FROM users AS u1 WHERE username is not null) sub WHERE row_num = 1",
		bun.Ident("users"), "username IS NOT NULL",
	).Scan(ctx, &rows)

	if err != nil {
		log.Fatalf("failed to fetch users: %v", err)
	}
}

func MapStructs(dst, src interface{}) {
	srcVal := reflect.ValueOf(src).Elem()
	dstVal := reflect.ValueOf(dst).Elem()

	for i := 0; i < srcVal.NumField(); i++ {
		field := srcVal.Type().Field(i).Name
		dstField := dstVal.FieldByName(field)
		if dstField.IsValid() && dstField.CanSet() {
			dstField.Set(srcVal.Field(i))
		}
	}
}

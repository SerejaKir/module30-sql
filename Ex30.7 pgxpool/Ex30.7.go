package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool" // command to download the pgx package and its dependencies
)

const (
	host     = "172.16.87.108"
	port     = 5432
	userDB   = "sergey"
	password = "password"
	dbname   = "postgres"
)

// структуры обычно используются для
// описания модели данных сущностей,
// хранящихся в БД
type book struct {
	id    int
	Year  int
	Title string
}

func main() {
	// методы драйвера PGX в нашем учебном приложении пустой
	var ctx context.Context = context.Background()

	// Подключение к БД. Функция возвращает объект БД.
	db, err := pgxpool.New(ctx, "postgres://"+userDB+":"+password+"@"+host+"/"+dbname)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	// Не забываем очищать ресурсы.
	defer db.Close()

	// Проверка соединения с БД. На случай, если sql.Open этого не делает.
	err = db.Ping(ctx)
	if err != nil {
		log.Fatal(err)
		//panic(err)
	}

	fmt.Println("Successfully connected!")

	data := []book{
		{Title: "Rob Pike", Year: 1985},
		{Title: "Ken Thompson", Year: 1885},
	}
	err = addBooks(ctx, db, data)
	if err != nil {
		log.Fatal(err)
	}
	users, err := users(db, ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(users)
}

// users возвращает всех пользователей.
func users(db *pgxpool.Pool, ctx context.Context) ([]book, error) {
	// запрос на выборку данных
	rows, err := db.Query(ctx, `
		SELECT * FROM users ORDER BY id;
	`)
	if err != nil {
		return nil, err
	}
	var users []book
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var u book
		err = rows.Scan(
			&u.id,
			&u.Title,
			&u.Year,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		users = append(users, u)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return users, rows.Err()
}

// addBooks добавляет в БД массив книг одной транзакцией.
func addBooks(ctx context.Context, db *pgxpool.Pool, books []book) error {
	// начало транзакции
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	// отмена транзакции в случае ошибки
	defer tx.Rollback(ctx)

	// пакетный запрос
	batch := new(pgx.Batch)
	// добавление заданий в пакет
	for _, book := range books {
		batch.Queue(`INSERT INTO books(title, year) VALUES ($1, $2)`, book.Title, book.Year)
	}
	// отправка пакета в БД (может выполняться для транзакции или соединения)
	res := tx.SendBatch(ctx, batch)
	// обязательная операция закрытия соединения
	err = res.Close()
	if err != nil {
		return err
	}
	// подтверждение транзакции
	return tx.Commit(ctx)
}

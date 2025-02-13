package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

const (
	host     = "172.16.87.108" //"localhost"
	port     = 5432
	userDB   = "sergey"
	password = "password"
	dbname   = "postgres"
)

// структуры обычно используются для
// описания модели данных сущностей,
// хранящихся в БД
type user struct {
	id   int
	name string
}

func main() {
	// Объект БД - пул подключений к СУБД.
	// БД - долгоживущий объект. Следует создавать только один объект для каждой БД.
	// Далее этот объект следует передавать как зависимость.
	var db *sql.DB
	var err error
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, userDB, password, dbname)

	// Подключение к БД.
	// В зависимости от драйвера, sql.Open может не выполнять фактического подключения,
	// а только проверить параметры соединения с БД.
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
		//panic(err)
	}
	// Не забываем очищать ресурсы.
	defer db.Close()

	// Проверка соединения с БД. На случай, если sql.Open этого не делает.
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
		//panic(err)
	}

	fmt.Println("Successfully connected!")

	data := []user{
		{name: "Rob Pike"},
		{name: "Ken Thompson"},
	}
	err = addUsers(db, data)
	if err != nil {
		log.Fatal(err)
	}
	users, err := users(db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(users)
}

// addUsers создает таблицу пользователей и заполняет данными.
func addUsers(db *sql.DB, users []user) error {
	// запрос на создание таблицы
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
		    id SERIAL PRIMARY KEY,
    		name TEXT NOT NULL
		);
	`)
	// не забываем проверять ошибки
	if err != nil {
		return err
	}
	for _, u := range users {
		// запрос на вставку данных
		_, err := db.Exec(`
		INSERT INTO users (name)
		VALUES ($1);
		`,
			u.name,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// users возвращает всех пользователей.
func users(db *sql.DB) ([]user, error) {
	// запрос на выборку данных
	rows, err := db.Query(`
		SELECT * FROM users ORDER BY id;
	`)
	if err != nil {
		return nil, err
	}
	var users []user
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var u user
		err = rows.Scan(
			&u.id,
			&u.name,
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

// addUsersTx добавляет пользователей в БД.
// Используется транзакция.
func addUsersTx(db *sql.DB, users []user) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// tx - объект транзакции; позволяет управлять ее работой
	for _, u := range users {
		// запрос на вставку данных
		_, err := tx.Exec(`
		INSERT INTO users (name)
		VALUES (?);
		`,
			u.name,
		)
		if err != nil {
			// откат транзакции в случае ошибки
			tx.Rollback()
			return err
		}
	}
	// фиксация (подтверждение) транзакции
	tx.Commit()
	return nil
}

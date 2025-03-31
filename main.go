package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

var db_url string = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", os.Getenv("DATABASE_USER"), os.Getenv("DATABASE_POSTGRES_PASSWORD"), os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_NAME"))

type OutputTransaction struct {
	ID            string    `json:"id"`
	AccountCode   string    `json:"account_code"`
	Description   string    `json:"description"`
	RawAmount     float64   `json:"raw_amount"`
	RoundedAmount int       `json:"rounded_amount"`
	Delta         float64   `json:"delta"`
	Timestamp     time.Time `json:"timestamp"`
}

type OutputSumDelta struct {
	SumDelta float64 `json:"sum_delta"`
}

type InputTransaction struct {
	AccountCode   string    `json:"account_code"`
	Description   string    `json:"description"`
	RawAmount     float64   `json:"raw_amount"`
	RoundedAmount int       `json:"rounded_amount"`
	Delta         float64   `json:"delta"`
	Timestamp     time.Time `json:"timestamp"`
}

type OutputTransactionBody struct {
	Date         string              `json:"date"`
	Transactions []OutputTransaction `json:"transactions"`
}

func addTransactions(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)
	db, err := pgxpool.New(context.Background(), db_url)
	if err != nil {
		fmt.Println("Unable to connect to database:", err)
	}
	defer db.Close()
	fmt.Println("Recieved request")
	var transactions []InputTransaction
	if err := json.NewDecoder(r.Body).Decode(&transactions); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		fmt.Println(err)
		return
	}

	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
		return
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	batch := &pgx.Batch{}
	query := `
        INSERT INTO "Rounder".transactions (account_code, description, raw_amount, rounded_amount, delta, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6::TIMESTAMP)`

	for _, txn := range transactions {
		batch.Queue(query, txn.AccountCode, txn.Description, txn.RawAmount, txn.RoundedAmount, txn.Delta, txn.Timestamp)
	}

	results := tx.SendBatch(ctx, batch)
	if err = results.Close(); err != nil {
		http.Error(w, "Batch insert failed", http.StatusInternalServerError)
		fmt.Println(err)
		return
	}

	if err = tx.Commit(ctx); err != nil {
		http.Error(w, "Transaction commit failed", http.StatusInternalServerError)
		fmt.Println(err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}

func getTransactions(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)
	dateGroups := make(map[string][]OutputTransaction)
	db, err := pgxpool.New(context.Background(), db_url)
	if err != nil {
		fmt.Println("Unable to connect to database:", err)

	}
	defer db.Close()
	fmt.Println("Recieved request")
	pageParam := r.URL.Query().Get("page")
	accountCode := r.URL.Query().Get("account_code")

	page, err := strconv.Atoi(pageParam)
	if err != nil || page <= 0 {
		http.Error(w, "Invalid page number", http.StatusBadRequest)
		return
	}

	limit := 25
	offset := (page - 1) * limit

	query := `SELECT id, account_code, description, raw_amount, rounded_amount, delta, timestamp FROM "Rounder".transactions WHERE account_code = $1 ORDER BY timestamp ASC LIMIT $2 OFFSET $3 `

	var rows pgx.Rows

	rows, err = db.Query(context.Background(), query, accountCode, limit, offset)

	if err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to retrieve transactions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var transactions []OutputTransaction

	for rows.Next() {
		var txn OutputTransaction
		if err := rows.Scan(&txn.ID, &txn.AccountCode, &txn.Description, &txn.RawAmount, &txn.RoundedAmount, &txn.Delta, &txn.Timestamp); err != nil {
			fmt.Println(err)
			http.Error(w, "Failed to scan transaction", http.StatusInternalServerError)
			return
		}
		transactions = append(transactions, txn)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Failed to iterate over transactions", http.StatusInternalServerError)
		return
	}

	for _, transaction := range transactions {

		onlyDate := time.Date(transaction.Timestamp.Year(), transaction.Timestamp.Month(), transaction.Timestamp.Day(), 0, 0, 0, 0, transaction.Timestamp.Location())

		curTransactionsInDateGroup := dateGroups[onlyDate.String()]
		curTransactionsInDateGroup = append(curTransactionsInDateGroup, transaction)
		dateGroups[onlyDate.String()] = curTransactionsInDateGroup
	}
	fmt.Println(dateGroups)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(dateGroups); err != nil {
		http.Error(w, "Failed to encode transactions", http.StatusInternalServerError)
		return
	}
}

func getTotalSaved(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)
	db, err := pgxpool.New(context.Background(), db_url)
	if err != nil {
		fmt.Println("Unable to connect to database:", err)

	}
	defer db.Close()
	fmt.Println("Recieved request")
	accountCode := r.URL.Query().Get("account_code")

	query := `SELECT SUM(delta) FROM "Rounder".transactions WHERE account_code = $1`

	var rows pgx.Rows

	rows, err = db.Query(context.Background(), query, accountCode)
	if err != nil {
		fmt.Println(err)
		http.Error(w, "Failed to retrieve transactions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var sumDelta OutputSumDelta
	for rows.Next() {
		if err := rows.Scan(&sumDelta.SumDelta); err != nil {
			fmt.Println(err)
			http.Error(w, "Failed to scan transaction", http.StatusInternalServerError)
			return
		}
		
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "Failed to iterate over transactions", http.StatusInternalServerError)
		return
	}

	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(sumDelta); err != nil {
		http.Error(w, "Failed to encode transactions", http.StatusInternalServerError)
		return
	}

}

func main() {

	fmt.Println("Listening")
	http.HandleFunc("GET /api/transactions", getTransactions)
	http.HandleFunc("GET /api/transactions/totalSaved", getTotalSaved)
	http.HandleFunc("POST /api/transactions", addTransactions)
	http.ListenAndServe(":3001", nil)

}

package repository

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"strings"
	todo "todo-gin"
)

type TodoListPostgres struct {
	db *sqlx.DB
}

func NewTodoListPostgres(db *sqlx.DB) *TodoListPostgres {
	return &TodoListPostgres{db: db}
}

func (r *TodoListPostgres) Create(userId int, list todo.TodoList) (int, error) {
	tx, err := r.db.Begin()

	if err != nil {
		return 0, err
	}

	var id int
	createListQuery := fmt.Sprintf("INSERT INTO %s (title, description) values ($1 $2) RETURNING id", todoListsTable)
	row := tx.QueryRow(createListQuery, list.Title, list.Description)
	if err := row.Scan(&id); err != nil {
		tx.Rollback()
		return 0, err
	}

	createUsersListQuery := fmt.Sprintf("INSERT INTO %s (user_id, list_id) values ($1 $2)", usersListsTable)
	_, err = tx.Exec(createUsersListQuery, userId, id)

	if err != nil {
		tx.Rollback()
		return 0, err
	}

	return id, tx.Commit()
}

func (r *TodoListPostgres) GetAll(userId int) ([]todo.TodoList, error) {
	var lists []todo.TodoList

	query := fmt.Sprintf("SELECT tl.id, tl.title, tl.description FROM %s tl INNER JOIN ul on tl.id = ul.list_id WHERE ul.user_id = $1", todoListsTable, usersListsTable)
	err := r.db.Select(&lists, query, userId)

	return lists, err

}

func (r *TodoListPostgres) GetById(userId, listId int) (todo.TodoList, error) {
	var list todo.TodoList

	query := fmt.Sprintf("SELECT tl.id, tl.title, tl.description FROM %s tl INNER JOIN ul on tl.id = ul.list_id WHERE ul.user_id = $1 AND ul.list_id = $2", todoListsTable, usersListsTable)
	err := r.db.Get(&list, query, userId, listId)

	return list, err
}

func (r *TodoListPostgres) Delete(userId, listId int) error {
	tx, err := r.db.Begin()

	if err != nil {
		return err
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = $1", todoListsTable)
	_, err = tx.Exec(deleteQuery, listId)

	if err != nil {
		tx.Rollback()
		return err
	}

	deleteUsersListQuery := fmt.Sprintf("DELETE FROM %s WHERE user_id = $1 AND list_id = $2", usersListsTable)
	_, err = tx.Exec(deleteUsersListQuery, userId, listId)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (r *TodoListPostgres) Update(userId, listId int, input todo.UpdateListInput) error {
	setValue := make([]string, 0)
	args := make([]interface{}, 0)
	argID := 1

	if input.Title != nil {
		setValue = append(setValue, fmt.Sprintf("title=$%d", argID))
		args = append(args, *input.Title)
		argID++
	}

	if input.Description != nil {
		setValue = append(setValue, fmt.Sprintf("description=$%d", argID))
		args = append(args, *input.Description)
		argID++
	}

	setQuery := strings.Join(setValue, ", ")

	query := fmt.Sprintf("UPDATE %s lt SET %s FROM %s ul WHERE tl.id = ul.list_id AND ul.list_id AND ul.list_id = $%d AND ul.user_id = $%d", todoListsTable, setQuery, usersListsTable, argID, argID+1)
	args = append(args, listId, userId)

	logrus.Debugf("update query: %s", query)
	logrus.Debugf("update args: %s", args)

	_, err := r.db.Exec(query, args...)

	return err
}

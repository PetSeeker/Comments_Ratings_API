from uuid import uuid4
from fastapi import HTTPException, Response
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from main import app, connect_db
import main

@pytest.fixture
def test_client():
    return TestClient(app)


@pytest.fixture
def mock_db_connection():
    with patch('main.psycopg2.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.__enter__.return_value = mock_cursor

        mock_connect.return_value = mock_connection

        yield mock_connection, mock_cursor

def test_connect_db_success(mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_connection.cursor.return_value = mock_cursor

    result = connect_db()

    assert result is True

def test_health(test_client):

    response = test_client.get("/health/")
    
    assert response.status_code == 200
    assert response.json() == {"status": "Server is healthy"}



def test_create_rating_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None
    mock_connection.cursor.return_value = mock_cursor


    unique_user_email = f"unique_user_{uuid4()}@example.com"
    unique_rater_email = f"unique_rater_{uuid4()}@example.com"

    form_data = {
        "user_email": unique_user_email,
        "rater_email": unique_rater_email,
        "rating": 5
    }

    with patch('main.connection', mock_connection):
        response = test_client.post("/ratings/", data=form_data)

    assert response.status_code == 200
    assert response.json()['message'] == "Rating created successfully"





def test_create_rating_invalid(test_client, mocker, mock_db_connection):
   mock_connection, mock_cursor = mock_db_connection
   mock_cursor.fetchone.return_value = None


   form_data = {
       "user_email": "unique_user@example.com",
       "rater_email": "unique_rater@example.com",
       "rating": 6 
   }

   response = test_client.post("/ratings/", data=form_data)

   assert response.json()["status_code"] == 400
   assert "Rating must be between 1 and 5." in response.text





def test_get_rating_id_found(test_client, mock_db_connection, mocker):
   mocker.patch("main.connect_db", return_value=True)

   mock_connection, mock_cursor = mock_db_connection
   mock_cursor.fetchone.return_value = (str(uuid4()),) 
   mock_connection.cursor.return_value = mock_cursor

   user_email = "user@example.com"
   rater_email = "rater@example.com"

   response = test_client.get(f"/ratings/?user_email={user_email}&rater_email={rater_email}")

   assert response.status_code == 200
   assert 'rating_id' in response.json()

def test_get_rating_id_not_found(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None  
    mock_connection.cursor.return_value = mock_cursor

    user_email = "user@example.com"
    rater_email = "rater@example.com"

    response = test_client.get(f"/ratings/?user_email={user_email}&rater_email={rater_email}")

    assert response.status_code == 200
    assert response.json() == {"rating_id": {}}



def test_delete_rating_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    rating_id = str(uuid4())
    mock_connection.cursor.return_value = mock_cursor

    response = test_client.delete(f"/ratings/{rating_id}")

    assert response.status_code == 200
    assert response.json() == {"message": "Rating deleted successfully"}
    


def test_update_rating_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    rating_id = str(uuid4())
    mock_connection.cursor.return_value = mock_cursor

    form_data = {
        "rating": 4
    }

    response = test_client.put(f"/ratings/{rating_id}", data=form_data)

    assert response.status_code == 200
    assert response.json() == {"message": "Rating updated successfully"}


def test_update_rating_invalid_value(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None

    rating_id = str(uuid4())
    form_data = {
        "rating": 6  
    }

    response = test_client.put(f"/ratings/{rating_id}", data=form_data)

    assert response.json()["status_code"] == 400
    assert "Rating must be between 1 and 5." in response.text
    

def test_get_rating_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    rating_id = str(uuid4())
    mock_cursor.fetchone.return_value = [4]  
    mock_connection.cursor.return_value = mock_cursor

    response = test_client.get(f"/ratings/{rating_id}")

    assert response.status_code == 200
    assert 'rating' in response.json()


def test_get_rating_not_found(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    rating_id = str(uuid4())
    mock_cursor.fetchone.return_value = None  
    mock_connection.cursor.return_value = mock_cursor

    with patch('main.connection', mock_connection):
        response = test_client.get(f"/ratings/{rating_id}")

    assert response.json()["status_code"] == 404
    assert "Rating not found" in response.text


def test_get_ratings_order_by_user(test_client):
    
    response = test_client.get("/ratings/user/")
    
    assert response.status_code == 200
    assert "users_ordered_by_rating" in response.json()



def test_get_user_ratings_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    user_email = "test_user@example.com"

    mock_cursor.fetchone.return_value = [4.5, 10, 'rater1@example.com:5,rater2@example.com:4', 1, 2, 3, 3, 1]
    mock_connection.cursor.return_value = mock_cursor
    
    with patch('main.connection', mock_connection):
        response = test_client.get(f"/ratings/user/{user_email}")

    expected_response = {
        "user_id": user_email,
        "average_rating": 4.5,
        "ratings_count": 10,
        "raters": [{'rater1@example.com': 5}, {'rater2@example.com': 4}],
        "star_percentages": [10, 20, 30, 30, 10]
    }
    
    assert response.status_code == 200
    assert response.json() == expected_response



def test_create_comment_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    form_data = {
        "comment": "Test comment",
        "commenter_email": "commenter@example.com",
        "listing_id": str(uuid4())
    }

    response = test_client.post("/comments/", data=form_data)

    assert response.status_code == 200
    assert response.json() == {"message": "Comment created successfully"}
 



def test_get_comments_and_replies(mocker, mock_db_connection):
   mock_connection, mock_cursor = mock_db_connection
   mock_cursor.fetchall.return_value = [("comment_id", "comment", "commenter_email", "created_at"), ("reply_id", "reply", "commenter_email", "created_at")]
   mock_connection.cursor.return_value = mock_cursor

   client = TestClient(app)
   response = client.get("/comments/123e4567-e89b-12d3-a456-426614174000") 

   assert response.status_code == 200
   assert "listing_data" in response.json()



def test_delete_comment_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_cursor.fetchall.return_value = None
    mock_connection.cursor.return_value = mock_cursor

    comment_id = str(uuid4())

    with patch('main.connection', mock_connection):
        response = test_client.delete(f"/comments/{comment_id}")

    assert response.status_code == 200
    assert response.json() == {"message": "Comment deleted successfully"}



def test_add_reply_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_connection.cursor.return_value = mock_cursor

    comment_id = str(uuid4())
    commenter_email = "reply@example.com"
    reply_content = "This is a reply."

    mock_cursor.execute.return_value = None

    response = test_client.post(
        f"/comments/{comment_id}/replies",
        data={
            "commenter_email": commenter_email,
            "reply": reply_content
        }
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Reply added successfully"}



def test_update_reply_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_connection.cursor.return_value = mock_cursor

    comment_id = str(uuid4())
    reply_id = str(uuid4())
    new_reply_content = "Updated reply content."

    mock_cursor.execute.return_value = None

    response = test_client.put(
        f"/comments/{comment_id}/replies/{reply_id}",
        data={"new_reply": new_reply_content}
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Reply updated successfully"}


def test_delete_reply_success(test_client, mock_db_connection):
    mock_connection, mock_cursor = mock_db_connection
    mock_connection.cursor.return_value = mock_cursor

    comment_id = str(uuid4())
    reply_id = str(uuid4())

    mock_cursor.execute.return_value = None

    response = test_client.delete(
        f"/comments/{comment_id}/replies/{reply_id}"
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Reply deleted successfully"}

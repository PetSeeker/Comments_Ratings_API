from uuid import UUID, uuid4
import psycopg2, os, logging
from fastapi import FastAPI, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

app = FastAPI(debug=True)

app.add_middleware(
    CORSMiddleware,
   allow_origins=[""],
   allow_credentials=True,
    allow_methods=[""],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(os.name)

connection = None


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_DATABASE = os.getenv("DB_DATABASE")

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    while not connect_db():
            continue

def connect_db():
    global connection, cursor
    try:
        connection = psycopg2.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_DATABASE)

        cursor = connection.cursor()
        if connection:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            logger.info(f"Connected to {db_version[0]}")
            create_tables()
            return True
        else:
            logger.error("Failed to connect to the database.")
            return False
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
        return False
    

@app.get("/health/")
async def health():
    return HTTPException(status_code=200, detail="Server is healthy")


#Ratings
@app.post("/ratings/")
async def create_rating(
    user_email: str = Form(...), 
    rater_email: str = Form(...), 
    rating: int = Form(...)
):
    global connection
    try:
        with connection.cursor() as cursor:
            
            if not 1 <= rating <= 5:
                return HTTPException(status_code=400, detail="Rating must be between 1 and 5.")

            check_query = """
                SELECT * FROM ratings WHERE user_email = %s AND rater_email = %s;
            """
            cursor.execute(check_query, (user_email, rater_email))
            existing_rating = cursor.fetchone()

            if existing_rating:
                return HTTPException(status_code=400, detail="Rating for the same user already exists.")

            insert_query = """
                INSERT INTO Ratings (user_email, rater_email, rating) VALUES (%s, %s, %s);
            """
            cursor.execute(insert_query, (user_email, rater_email, rating))
            connection.commit()

            return {"message": "Rating created successfully"}
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/ratings/")
async def get_rating_id(user_email: str, rater_email: str):
    global connection
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT rating_id FROM ratings WHERE user_email = %s AND rater_email = %s;
            """
            cursor.execute(query, (user_email, rater_email))
            result = cursor.fetchone()

            if result:
                rating_id = result[0]
                return {"rating_id": rating_id}
            else:
                return {"rating_id": None}

    except Exception as e:
        logger.error(f"Error retrieving rating ID: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    
@app.delete("/ratings/{rating_id}")
async def delete_rating(rating_id: UUID):
    global connection
    try:
        with connection.cursor() as cursor:
            
            delete_query = """
                DELETE FROM Ratings WHERE rating_id = %s;
            """
            cursor.execute(delete_query, (str(rating_id),))
     
            connection.commit()

            return {"message": "Rating deleted successfully"}

    except Exception as e:
        connection.rollback()
        logger.error(f"Error deleting rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    

@app.put("/ratings/{rating_id}")
async def update_rating(
    rating_id: UUID, 
    rating: int = Form(...)
):
    global connection
    try:
        with connection.cursor() as cursor:
      
            if not 1 <= rating <= 5:
                return HTTPException(status_code=400, detail="Rating must be between 1 and 5.")

            update_query = """
                UPDATE Ratings SET Rating = %s WHERE rating_id = %s;
            """
            cursor.execute(update_query, (rating, str(rating_id)))
            
            connection.commit()
            
            return {"message": "Rating updated successfully"}

    except Exception as e:
        connection.rollback()
        logger.error(f"Error updating rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/ratings/{rating_id}")
async def get_rating(rating_id: UUID):
    global connection
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT rating FROM ratings WHERE rating_id = %s;
            """
            cursor.execute(query, (str(rating_id),))

            rating = cursor.fetchone()[0]

            return {"Rating retrieved successfully": rating}

    except Exception as e:
        connection.rollback()
        logger.error(f"Error retrieving rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/ratings/user/{user_email}")
async def get_user_ratings(user_email: str):
    global connection
    try:
        with connection.cursor() as cursor:
            
            query = """
                SELECT 
                    AVG(rating) as average_rating, 
                    COUNT(*) as ratings_count, 
                    STRING_AGG(DISTINCT CONCAT(rater_email, ':', Rating), ',') as raters,
                    COUNT(CASE WHEN rating = 1 THEN 1 ELSE NULL END) as count_1_star,
                    COUNT(CASE WHEN rating = 2 THEN 1 ELSE NULL END) as count_2_stars,
                    COUNT(CASE WHEN rating = 3 THEN 1 ELSE NULL END) as count_3_stars,
                    COUNT(CASE WHEN rating = 4 THEN 1 ELSE NULL END) as count_4_stars,
                    COUNT(CASE WHEN rating = 5 THEN 1 ELSE NULL END) as count_5_stars
                FROM ratings 
                WHERE user_email = %s;
            """
            cursor.execute(query, (user_email,))
            result_set = cursor.fetchone()

            if result_set:
        
                average_rating = result_set[0]  
                ratings_count = result_set[1]   
                raters_str = result_set[2]      

                raters = [{item.split(':')[0]: int(item.split(':')[1])} for item in raters_str.split(',')] if raters_str else []

                star_percentages = [round(result_set[i + 3] / ratings_count * 100) if ratings_count != 0 else 0 for i in range(5)]

                return {
                    "user_id": user_email,
                    "average_rating": float(average_rating) if average_rating is not None else None,
                    "ratings_count": ratings_count,
                    "raters": raters,
                    "star_percentages": star_percentages
                }
            else:
                return HTTPException(status_code=404, detail="User not found")
            
    except Exception as e:
        logger.error(f"Error retrieving user ratings information: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    


#Comments
@app.post("/api/comments/")
async def create_comment(comment: str = Form(...), user_id: str = Form(...), commenter_id: str = Form(...), listing_id: str = Form(...)):
    global connection
    try:
        with connection.cursor() as cursor:
            insert_query = """
                INSERT INTO Comments (comment_id, Comment, user_id, commenter_id, listing_id)
                VALUES (%s, %s, %s, %s, %s);
            """
            comment_id = str(uuid4())
            cursor.execute(insert_query, (comment_id, comment, user_id, commenter_id, listing_id))
            connection.commit()
            return {"comment_id": comment_id, "message": "Comment created successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))
    


@app.get("/api/comments/{listing_id}")
async def get_comments(listing_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            select_query = """
                SELECT * FROM Comments WHERE listing_id = %s;
            """
            cursor.execute(select_query, (listing_id,))
            comments = cursor.fetchall()

            formatted_comments = [
                {
                    "comment_id": comment[0],
                    "comment": comment[1],
                }
                for comment in comments
            ]

            return {"listing_id": listing_id, "comments": formatted_comments}
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))
    


@app.put("/api/comments/{comment_id}")
async def update_comment(comment_id: str, new_comment: str = Form(...)):
    global connection
    try:
        with connection.cursor() as cursor:
            update_query = """
                UPDATE Comments SET Comment = %s WHERE comment_id = %s;
            """
            cursor.execute(update_query, (new_comment, comment_id))
            connection.commit()
            return {"comment_id": comment_id, "message": "Comment updated successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))



@app.delete("/api/comments/{comment_id}")
async def delete_comment(comment_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            delete_query = """
                DELETE FROM Comments WHERE comment_id = %s;
            """
            cursor.execute(delete_query, (comment_id,))
            connection.commit()
            return {"comment_id": comment_id, "message": "Comment deleted successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))



#Replies
@app.get("/api/comments/{comment_id}/replies")
async def get_replies(comment_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            select_query = """
                SELECT * FROM Replies WHERE comment_id = %s;
            """
            cursor.execute(select_query, (comment_id,))
            replies = cursor.fetchall()

        formatted_replies = [
                {
                    "reply_id": reply[0],
                    "reply": reply[1],
                }
                for reply in replies
            ]
        
        return {"comment_id": comment_id, "replies": formatted_replies}
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))


@app.post("/api/comments/{comment_id}/replies")
async def add_reply(comment_id: str, user_id: str = Form(...), reply: str = Form(...)):
    global connection
    try:
        with connection.cursor() as cursor:
            insert_query = """
                INSERT INTO Replies (reply_id, reply, user_id, comment_id)
                VALUES (%s, %s, %s, %s);
            """
            reply_id = str(uuid4())
            cursor.execute(insert_query, (reply_id, reply, user_id, comment_id))
            connection.commit()
            return {"reply_id": reply_id, "message": "Reply added successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))


@app.put("/api/comments/{comment_id}/replies/{reply_id}")
async def update_reply(comment_id: str, reply_id: str, new_reply: str = Form(...)):
    global connection
    try:
        with connection.cursor() as cursor:
            update_query = """
                UPDATE Replies SET reply = %s WHERE reply_id = %s AND comment_id = %s;
            """
            cursor.execute(update_query, (new_reply, reply_id, comment_id))
            connection.commit()
            return {"reply_id": reply_id, "message": "Reply updated successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))
    

@app.delete("/api/comments/{comment_id}/replies/{reply_id}")
async def delete_reply(comment_id: str, reply_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            delete_query = """
                DELETE FROM Replies WHERE reply_id = %s AND comment_id = %s;
            """
            cursor.execute(delete_query, (reply_id, comment_id))
            connection.commit()
            return {"reply_id": reply_id, "message": "Reply deleted successfully"}
    except Exception as e:
        connection.rollback()
        return HTTPException(status_code=500, detail=str(e))



    
def create_tables():
    try:
        global connection,cursor

        create_ratings_table = """
            CREATE TABLE IF NOT EXISTS Ratings (
                rating_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
	            rating INT NOT NULL CHECK (Rating BETWEEN 1 AND 5),
                user_email VARCHAR NOT NULL,
                rater_email VARCHAR NOT NULL
            );
        """

        cursor = connection.cursor()
        cursor.execute(create_ratings_table)

        create_comments_table = """
            CREATE TABLE IF NOT EXISTS Comments (
                comment_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                Comment TEXT NOT NULL,
                user_id VARCHAR NOT NULL,
                commenter_id VARCHAR NOT NULL,
                listing_id VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        cursor = connection.cursor()

        cursor.execute(create_comments_table)

        create_replies_table = """
        CREATE TABLE IF NOT EXISTS Replies (
            reply_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            reply TEXT NOT NULL,
            user_id VARCHAR NOT NULL,
            comment_id UUID REFERENCES Comments(comment_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
       
        cursor.execute(create_replies_table)

       
        connection.commit()
        logger.info("Tables created successfully in PostgreSQL database")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating tables: {error}")


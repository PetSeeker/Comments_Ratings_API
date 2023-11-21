from uuid import UUID
import psycopg2, os, logging
from fastapi import FastAPI, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware


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

DB_USER = "docker"
DB_PASSWORD = "docker"
DB_HOST = "database"
DB_PORT = "5432"
DB_DATABASE = "exampledb"

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


@app.post("/ratings/")
async def create_rating(
    user_id: str = Form(...), 
    rater_id: str = Form(...), 
    rating: int = Form(...)
):
    global connection
    try:
        with connection.cursor() as cursor:
            if not 1 <= rating <=5:
                return HTTPException(status_code=400, detail="Rating must be between 1 and 5.")


            rating_id = insert_rating_data(cursor, user_id, rater_id, rating)
            connection.commit()



            return {"message": "Rating created successfully", "rating_id": rating_id}
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    
    
@app.get("/user/{user_id}/average_rating")
async def get_user_average_rating(user_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT AVG(Rating) FROM Ratings WHERE UserID = %s;
            """
            cursor.execute(query, [user_id])
            average_rating = cursor.fetchone()[0]

            if average_rating is not None:
                return {"user_id": user_id, "average_rating": float(average_rating)}
            else:
                return HTTPException(status_code=404, detail="User not found or no ratings available")
    except Exception as e:
        logger.error(f"Error retrieving user average rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")

    

@app.delete("/ratings/{rating_id}")
async def delete_rating(rating_id: UUID):
    global connection
    try:
        with connection.cursor() as cursor:
            
            delete_query = """
                DELETE FROM Ratings WHERE id = %s RETURNING ID;
            """
            cursor.execute(delete_query, (str(rating_id),))
            deleted_id = cursor.fetchone()

            if deleted_id:
                connection.commit()
                return {"message": "Rating deleted successfully", "rating_id": deleted_id[0]}
            else:
                return HTTPException(status_code=404, detail="Rating not found")
    except Exception as e:
        connection.rollback()
        logger.error(f"Error deleting rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    


@app.put("/ratings/{rating_id}")
async def update_rating(
    rating_id: UUID, 
    rating: int
):
    global connection
    try:
        with connection.cursor() as cursor:
            # Verifica se a nova avaliação está no intervalo permitido
            if not 1 <= rating <= 5:
                return HTTPException(status_code=400, detail="Rating must be between 1 and 5.")

            # Atualiza a avaliação no banco de dados
            update_query = """
                UPDATE Ratings SET Rating = %s WHERE id = %s RETURNING ID;
            """
            cursor.execute(update_query, (rating, str(rating_id)))
            updated_id = cursor.fetchone()

            if updated_id:
                connection.commit()
                return {"message": "Rating updated successfully", "rating_id": updated_id[0]}
            else:
                return HTTPException(status_code=404, detail="Rating not found")
    except Exception as e:
        connection.rollback()
        logger.error(f"Error updating rating: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/user/{user_id}/ratings/count")
async def get_user_ratings_count(user_id: str):
    global connection
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT COUNT(*) FROM Ratings WHERE UserID = %s;
            """
            cursor.execute(query, (user_id,))
            count = cursor.fetchone()[0]

            return {"user_id": user_id, "ratings_count": count}
    except Exception as e:
        logger.error(f"Error retrieving user ratings count: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")

def create_tables():
    try:
        global connection,cursor

        # Create the ratings table 
        create_ratings_table = """
            CREATE TABLE IF NOT EXISTS Ratings (
                rating_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
	            Rating INT NOT NULL CHECK (Rating BETWEEN 1 AND 5),
                user_email VARCHAR NOT NULL,
                rater_rmail VARCHAR NOT NULL
            );
        """

        cursor = connection.cursor()
        cursor.execute(create_ratings_table)
        connection.commit()
        logger.info("Tables created successfully in PostgreSQL database")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating tables: {error}")




def insert_rating_data(cursor, user_id, rater_id, rating):
    insert_query = """
        INSERT INTO Ratings (UserID, RaterID, Rating) VALUES (%s, %s, %s) RETURNING ID;
    """
    cursor.execute(insert_query, (user_id, rater_id, rating))
    rating_id = cursor.fetchone()[0]
    return rating_id

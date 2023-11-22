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
    user_email: str = Form(...), 
    rater_email: str = Form(...), 
    rating: int = Form(...)
):
    global connection
    try:
        with connection.cursor() as cursor:
            if not 1 <= rating <= 5:
                return HTTPException(status_code=400, detail="Rating must be between 1 and 5.")


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

@app.get("/ratings/{user_email}")
async def get_user_ratings(user_email: str):
    global connection
    try:
        with connection.cursor() as cursor:
            
            average_query = """
                SELECT AVG(Rating) FROM Ratings WHERE user_email = %s;
            """
            cursor.execute(average_query, (user_email,))
            average_rating = cursor.fetchone()[0]

            count_query = """
                SELECT COUNT(*) FROM Ratings WHERE user_email = %s;
            """
            cursor.execute(count_query, (user_email,))
            ratings_count = cursor.fetchone()[0]

            raters_query = """
                SELECT DISTINCT rater_email FROM Ratings WHERE user_email = %s;
            """
            cursor.execute(raters_query, (user_email,))
            raters = cursor.fetchall()

            result = {
                "user_id": user_email,
                "average_rating": float(average_rating) if average_rating is not None else None,
                "ratings_count": ratings_count,
                "raters": [rater[0] for rater in raters]
            }
            return result

    except Exception as e:
        logger.error(f"Error retrieving user ratings information: {e}")
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
                return HTTPException(status_code=404, detail="Rating not found")

    except Exception as e:
        logger.error(f"Error retrieving rating ID: {e}")
        return HTTPException(status_code=500, detail="Internal Server Error")
    
def create_tables():
    try:
        global connection,cursor

        # Create the ratings table 
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
        connection.commit()
        logger.info("Tables created successfully in PostgreSQL database")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating tables: {error}")


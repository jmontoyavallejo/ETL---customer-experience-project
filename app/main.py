from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from app.modules.Extraction import full_extraction
from app.modules.Tranformation import Big_query_executor


app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:3000",
    "any https:// desired to fetch from the api",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/",response_class=HTMLResponse)
def read_root():
    return '''<html>
    <div style="color: black;box-shadow: 8px 8px 5px #444;padding:5em;border: 1px solid #333;text-align: center;margin: auto;background-image: linear-gradient(180deg, #fff, #ddd 40%, #ccc);width: 20em;border: 1px solid #333;font-size: 23px;"> 
    Hello! welcome <br> to Juan Pablo's ETL customer experience postgresql repo, feel free to go to 
    <a href="http://localhost:8000/docs">Fastapi Docs</a>. 
    so you can interact with the repository, you must have access to the google cloud project server, send and email request for access,download google SDK and run this script: gcloud auth application-default login 
    </div>
    </html>
    '''


@app.get("/extraction")
async def extraction():
    full_extraction()
    return "Raw Table's upload complete"

@app.get("/tranformation")
async def full_extraction():
    
    with open('app/SQL/Query_clientes_simplificado.sql') as file:
        query_clientes_simplificado=file.read()
    
    with open('app/SQL/Query_contactable.sql') as file:
        query_contactables=file.read()
    
    with open('app/SQL/Query_mineria_almacen.sql') as file:
        query_mineria_almacen=file.read()
    
    with open('app/SQL/Query_mineria_campanas.sql') as file:
        query_mineria_campanas=file.read()
    
    with open('app/SQL/Query_mineria_productos.sql') as file:
        query_mineria_productos=file.read()
    
    with open('app/SQL/Query_mineria_ventas.sql') as file:
        query_mineria_ventas=file.read()

    with open('app/SQL/Query_blacklist.sql') as file:
        query_blacklist=file.read()
    
    bq=Big_query_executor(project_name='customer-experience-384423')
    
    bq.execute_query(query_mineria_ventas)
    bq.execute_query(query_contactables)
    bq.execute_query(query_mineria_almacen)
    bq.execute_query(query_mineria_campanas)
    bq.execute_query(query_mineria_productos)
    bq.execute_query(query_blacklist)
    bq.execute_query(query_clientes_simplificado)

    return "Transform Table's upload complete"
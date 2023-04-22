from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from modules.Extraction import full_extraction
app = FastAPI()


@app.get("/",response_class=HTMLResponse)
def read_root():
    return '''<html>
    <div style="color: black;box-shadow: 8px 8px 5px #444;padding:5em;border: 1px solid #333;text-align: center;margin: auto;background-image: linear-gradient(180deg, #fff, #ddd 40%, #ccc);width: 20em;border: 1px solid #333;font-size: 23px;"> 
    Hello! welcome <br> to Juan Pablo's ETL customer experience postgresql repo, feel free to go to 
    <a href="http://localhost:8000/docs">Fastapi Docs</a>. 
    so you can interact with the repository  
    </div>
    </html>
    '''


@app.get("/extraction")
def extraction():
    full_extraction()
    return "Raw Table's upload complete"
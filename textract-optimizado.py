#Usar textract para obtener la CURP de una INE tanto actual como antigua
import sys # acceso a variable y funciones usadas y mantenidas por el interpete
import traceback # funciones para imprimir o recuperar informacion sobre excepciones y trazas de pila
import logging # Funciones para registrar mensajes en diferentes niveles
import json # Funciones para trabajar en formato jsn
import uuid # proporciona funciones para generar identificadores universales
import boto3 # Proporciona una interfaz de cliente para interactuar con servicios aws
from urllib.parse import unquote_plus # Proporciona funciones para realizar analisis y codificacion de url

logger = logging.getLogger()  # Crea un objeto de registro
logger.setLevel(logging.INFO)  # Establece el nivel de registro en INFO

def process_error() -> dict:
    
    #obtiene informacion sobre la excepcion actual y genera un mensaje de error estructurado en formato JSON
    ex_type, ex_value, ex_traceback = sys.exc_info()
    traceback_string = traceback.format_exception(ex_type, ex_value, ex_traceback)
    error_msg = json.dumps(
        {
            "errorType": ex_type.__name__,
            "errorMessage": str(ex_value),
            "stackTrace": traceback_string,
        }
    )
    return error_msg
    
def extract_curp(response: dict) -> str:
    contador = (-2) # se inicia un contador en numeros negativos puesto que no sabemos la cantidad final de iteraciones
    for block in response["Blocks"]: # se itera para revisar los campos con intencion de encontrar CURP en el campo de texto
        if block["BlockType"] == "LINE" and "Text" in block and "CURP" in block["Text"]:
            n2 = len(block["Text"]) # obtenemos la cantidad de caracteres del campo actual
            if n2<18: # se verifica si la credencial es nueva o antigua esto tomando en cuenta que las antiguas tienene la curp
                      # en el mismo campo que el titulo "CURP", y las nuevas en otro campo entonces se comprueban la cantidad de 
                      # caracteres y si es menor que 15 los cuales son los caracteres minimos de una curp detecta que es una 
                        # credencial de edicion nueva
                print("Nueva")
                contador = 2 # Contador para la cantidad de lineas que tendra que saltar para encontrar el campo de la curp
            else:
                print("Antigua")
                return block["Text"] # retornamos la CURP en el caso de que sea la antigua
        if (contador == 0): # se comprueba el contador para detectar si ya estamos en el campo de la curp
            n1 = len(block["Text"]) # obtenemos la cantidad de caracteres del campo actual
            if (n1 !=18 ): # comprobamos que efectivamente sea la curp y no detecte la firma como campo extra
                contador+=1
            else:
                return block["Text"] # retornamos la curp de una credencial INE nueva edicion
        contador -= 1 #se reduce el contador en uno
    return "No se encontro la curp" # en caso de que no retorne nada retornamos un mensaje

    
def lambda_handler(event, context):
    textract = boto3.client("textract") # se crea el cliente para conectar con textract
    s3 = boto3.client("s3") # se crea el ciente para conectar con s3
    
    try:
        if "Records" in event:
            file_obj = event["Records"][0] # almacenamos los records en una variable para poder usarlos
            bucketname = str(file_obj["s3"]["bucket"]["name"]) # sacamos el nombre del bucket 
            filename = unquote_plus(str(file_obj["s3"]["object"]["key"])) # obtenemos el nombre del archive
            
            logging.info(f"Bucket: {bucketname} ::: Key: {filename}") #Opcional
            
            response = textract.detect_document_text( # almacenamos la informacion obtenida de textract
                Document={
                    "S3Object": {
                        "Bucket": bucketname, # se especifica el nombre del bucket
                        "Name": filename  # Agregar esta línea para especificar el nombre del archivo
                    }
                }
            )
        
            # Registrar el resultado el resultado de la deteccion del texto en el documento (Opcional)
            logging.info(json.dumps(response))
            
             # Extrae la CURP del texto extraido
            curp_test = extract_curp(response)
            curp_text = curp_test
            #curp_text = curp_test.split(' ')[-1] # aqui se elimina la parte previa a la curp

            if curp_text == "No se encontro la curp": # validacion para evitar crear documentos si no se encontro la curp
                print(curp_text)
            else:
                #imprime la CURP extraida
                print("Hola la CURP es: ", curp_text)

                #Guarda la CURP en un archivo de texto en el bucket de amazon S3
                s3.put_object(
                    Bucket=bucketname, # se asigna el bucket en el que se almacenara
                    Key=f"output/{filename.split('/')[-1]}_{uuid.uuid4().hex}.txt", # Se crea la ruta para almacenar el archivo en el Bucket
                    Body=str(curp_text), # Aqui se asigna la informacion que ira en el cuerpo del documeto
                )
                
                return {
                    "statusCode": 200,
                    "body": json.dumps("Document processed successfully!"),
                }
    except Exception as e:
        error_msg = process_error()
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps("Error processing the document!")}
    
#Trujillo Garcia Sergio Andres
#sergio_truga@hotmail.com
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# Conexión a AWS C2
session = boto3.session.Session(
   aws_access_key_id=os.getenv("ACCESS_KEY"),
   aws_secret_access_key=os.getenv("SECRET_KEY"),
   aws_session_token=os.getenv("SESSION_TOKEN"),
   region_name=os.getenv("REGION"))

ec2 = session.client('ec2')


# Crear instancia EC2
def crear_ec2():
    instancia = ec2.run_instances(
        ImageId=os.getenv("IMAGE_ID"), 
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
    )
    
    instancia_id = instancia['Instances'][0]['InstanceId']
    
    print("Instancia EC2 creada:", instancia_id)

    return instancia_id

# Ejecutar instancia EC2
def ejecuar_ec2(instance_id):
    ec2.start_instances(InstanceIds=[instance_id])
    print("Arrancando instancia...")

    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    print("Instancia arrancada y en ejecución.")

# Parar instancia EC2
def parar_ec2(instance_id):
    ec2.stop_instances(InstanceIds=[instance_id])
    print("Parando instancia...")

    waiter = ec2.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=[instance_id])
    print("Instancia parada.")

def eliminar_ec2(instance_id):
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("Eliminando instancia...")

    waiter = ec2.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[instance_id])
    print("Instancia eliminada.")

instancia_id = crear_ec2()
ejecuar_ec2(instancia_id)
parar_ec2(instancia_id)
eliminar_ec2(instancia_id)
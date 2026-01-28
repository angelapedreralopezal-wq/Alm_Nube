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
        Placement={
            'AvailabilityZone': 'us-east-1a' 
        }
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

# Eliminar instancia EC2
def eliminar_ec2(instance_id):
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("Eliminando instancia...")

    waiter = ec2.get_waiter('instance_terminated')
    waiter.wait(InstanceIds=[instance_id])
    print("Instancia eliminada.")

# Crear volumen EBS
def crear_ebs(tamano_gb=10, zona_disponibilidad='us-east-1a', tipo='gp3', nombre='MiVolumenEBS'):
    # Si no se especifica zona, usar la de la sesión por defecto
    if not zona_disponibilidad:
        # Obtenemos la primera zona disponible en la región
        zonas = ec2.describe_availability_zones()['AvailabilityZones']
        zona_disponibilidad = zonas[0]['ZoneName']

    volumen = ec2.create_volume(
        AvailabilityZone=zona_disponibilidad,
        Size=tamano_gb,
        VolumeType=tipo,
        TagSpecifications=[
            {
                'ResourceType': 'volume',
                'Tags': [
                    {'Key': 'Name', 'Value': nombre}
                ]
            }
        ]
    )
    
    volumen_id = volumen['VolumeId']
    print(f"Volumen EBS creado: {volumen_id} (Zona: {zona_disponibilidad}, Tamaño: {tamano_gb}GB, Tipo: {tipo})")
    return volumen_id

# Asociar volumen EBS a instancia EC2
def asociar_ebs_a_ec2(instance_id, volumen_id, dispositivo='/dev/sdf'):
    # Esperar a que el volumen esté disponible
    print(f"Esperando a que el volumen {volumen_id} esté disponible...")
    waiter = ec2.get_waiter('volume_available')
    waiter.wait(VolumeIds=[volumen_id])
    print(f"Volumen {volumen_id} ya está disponible, procediendo a adjuntar...")

    # Adjuntar el volumen
    ec2.attach_volume(
        InstanceId=instance_id,
        VolumeId=volumen_id,
        Device=dispositivo
    )
    print(f"Volumen {volumen_id} adjuntado a la instancia {instance_id} en {dispositivo}.")

    # Esperar a que el volumen esté in-use
    waiter_in_use = ec2.get_waiter('volume_in_use')
    waiter_in_use.wait(VolumeIds=[volumen_id])
    print(f"Volumen {volumen_id} ahora está en uso.")

instancia_id = crear_ec2()
ejecuar_ec2(instancia_id)

volumen_id = crear_ebs(tamano_gb=20, tipo='gp3', nombre='VolumenEjemplo')
asociar_ebs_a_ec2(instancia_id, volumen_id)

# parar_ec2(instancia_id)
# eliminar_ec2(instancia_id)
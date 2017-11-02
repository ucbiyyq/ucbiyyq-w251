import object_storage
import timeit

def list_objects():
    container_list = sl_storage.containers()
    for container in container_list:
        print(container.name)
        print(sl_storage[container.name].objects())
        
    
def upload_objects():
    for container_name in containers:
        sl_storage[container_name].create()
        #sl_storage[container_name][filename].create()
        sl_storage[container_name][filename].send('Plain-Text Content')

        
def read_objects():
    container_list = sl_storage.containers()
    for container in container_list:
        objects = sl_storage[container.name].objects()
        for obj in objects:
            print("... reading obj", obj)
            obj.read()
        
        
def delete_objects():
    container_list = sl_storage.containers()
    for container in container_list:
        sl_storage[container.name][filename].delete()
        sl_storage[container.name].delete()

        
# preps the softlayer storage objects
sl_storage = object_storage.get_client('SLOS1432883-2:SL1432883', '674c1cddcc2458f3400ba9f5f0066db87c056c5cff8bb1fcf85a5a0eec0b2ffd', datacenter='dal05')
containers = ["container1", "container2", "container3"]
filename = "file.txt"
    
# lists current containers
print("")
print("current containers")
list_objects()

# uploads objects
print("")
print("uploading objects")
timeit.timeit(upload_objects)

# lists new containers
print("")
print("new containers")
list_objects()

# reads objects
print("")
print("reading objects")
#timeit.timeit(read_objects)

# cleans up objects
print("")
print("deleting objects")
delete_objects()

# lists containers after deletion
print("")
print("containers after deletion")
list_objects()
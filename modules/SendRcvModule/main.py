# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import os
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient
from azure.iot.device import Message
import uuid
import json
import datetime

async def main():
    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()


        # define behavior for receiving an input message on input1
        async def send_message(module_client):
            print("Running Send Message")
            payload = {
                    "timestamp" : str(time.time()),
                    "inferences" : [
                            {      
                                "type": "text" ,
                                "subtype": "",
                                "inferenceId": "",
                                "relatedInferences": [],
                                "text": {
                                        "value":"gASVDA4AAAAAAAB9lCiMBWJveGVzlIwVbnVtcHkuY29yZS5tdWx0aWFycmF5lIwMX3JlY29uc3Ry\\ndWN0lJOUjAVudW1weZSMB25kYXJyYXmUk5RLAIWUQwFilIeUUpQoSwFLAUsEhpRoBYwFZHR5cGWU\\nk5SMAmY0lImIh5RSlChLA4wBPJROTk5K/////0r/////SwB0lGKJQxC/t8JEoQFURIFv00Tkc25E\\nlHSUYowGc2NvcmVzlGgEaAdLAIWUaAmHlFKUKEsBSwGFlGgRiUMEAvh/P5R0lGKMB2NsYXNzZXOU\\naARoB0sAhZRoCYeUUpQoSwFLAYWUaA6MAmk4lImIh5RSlChLA2gSTk5OSv////9K/////0sAdJRi\\niUMIAQAAAAAAAACUdJRijAZsYWJlbHOUaARoB0sAhZRoCYeUUpQoSwFLAYWUaA6MA1UxNJSJiIeU\\nUpQoSwNoEk5OTks4SwRLCHSUYolDOGwAAAB1AAAAZwAAAGcAAABhAAAAZwAAAGUAAAB0AAAAcgAA\\nAGEAAABpAAAAbAAAAGUAAAByAAAAlHSUYowFbWFza3OUaARoB0sAhZRoCYeUUpQoSwFLAUscSxyH\\nlGgRiUJADAAAy6OKOvpBGDxy/wI+g1UAP/yiJD9MiFc/96dsPzY8dD81mW8/V/hwPxUXaT+e8Wg/\\nmU9vP22CaT+mHG0/g3pqP+wyaj9FoGM/5BFdP7ZfST80MyI/3DZCPqt/Uzz3juw5z3CUOPtrSTeM\\no1E3xTrKNjp2vzrCPd88Z5IxPzN1dT8p0H0/EZB/P4fxfz9E+n8/AP1/P/z9fz84/n8/Xv5/P3z+\\nfz/4/X8/tP1/PzL9fz9A/H8/vvp/P/T3fz9V8H8/ncZ/P6M/fT8sOtk+huv7OwoEnTlhXOE35+mz\\nNzpvBTftCiA6y6VMPaDLbD8v6H0/S4t/P53zfz9C/38/zv9/P/b/fz/8/38//v9/P/7/fz/+/38/\\n/v9/P/z/fz/6/38/+v9/P/j/fz/0/38/5v9/P4L/fz8m938/epxuP1jB7jy3KwU6Hbp4N0IyDjdn\\nKCs2QqeEOi2YET5fyXo/OWJ/P3jefz+S/H8/3P9/P/b/fz/+/38/AACAPwAAgD8AAIA/AACAPwAA\\ngD8AAIA/AACAPwAAgD8AAIA//v9/P/7/fz/0/38/YP9/P4Dtfj8LKo0+Rjw7OzWyKzgyNpY3r9yP\\nNu3e6zpS2fM+ICt/Pxjefz9p8X8/CP9/P+z/fz/8/38//v9/PwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAPwAAgD8AAIA/AACAPwAAgD/+/38//v9/P+L/fz9Q9X8/IfFwP9FxHD1rKxo5lJ/vNzsaqzZo\\nB5E7C+g/Pw6xfz+N7n8/HPp/P5j/fz/6/38//v9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD/6/38/3P5/PwM9fz8mVQc/Wu9gO4i+MzmFVow3ReqH\\nPI6Vcj+o638/tvp/P6z8fz/G/38/+P9/P/7/fz8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAPwAAgD8AAIA/AACAPwAAgD8AAIA/+v9/P8j/fz8s9n8/wwN7P256BT6fANc6frORONIVTj0n\\nZ3s/GvZ/PzL8fz88/n8/3P9/P/z/fz8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD8AAIA/AACAPwAAgD8AAIA/AACAP/7/fz/w/38//P5/P6vPfz89EWI/Kfw1PQTvcDps5JE+vCN/\\nP8D8fz9+/n8/xP5/P+b/fz/6/38/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAPwAAgD/+/38//P9/P/r/fz/s/38/1P9/P1L/fz/W+38/Or5+P4mPBj+Wkk880NgSP9q7fz+G\\n/n8/Lv9/P4r/fz/2/38//v9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD8AAIA//v9/P/r/fz/2/38/6v9/P+b/fz++/38/vP5/Pyq4fz9PRlQ/KR5QPS75aD8L7n8/av9/\\nP7b/fz+4/38//P9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAP/7/fz/8/38/+P9/P+7/fz/g/38/tP9/P2b/fz935H8/+ep0P7rLTz7qF3E/C/N/P5r/fz/Q\\n/38/2v9/P/7/fz8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD/+/38//P9/P/z/fz/2/38/8v9/P+b/fz+y/38/DvZ/P3USez9VrKs+iGl9P/76fz+o/38/3v9/\\nP9j/fz/+/38/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAP/7/fz/+/38//P9/P/j/fz/u/38/zP9/P1r4fz+UxHs/hYCTPlUffj9G+38/sP9/P+D/fz/c\\n/38//v9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD/+/38//v9/P/7/fz/8/38/+P9/P9j/fz+w+n8//S57P5scUT5+TH8/1Px/P6L/fz/e/38/tP9/\\nP/r/fz/+/38/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\nAACAPwAAgD/+/38//v9/P/r/fz/Y/38/mvV/P3tqbz8t4lE9AmV/P3T8fz+U/38/0v9/P6T/fz/2\\n/38//P9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAA\\ngD8AAIA/AACAP/7/fz/8/38/zP9/P3fwfz9jnFo/+qeGPDiNfz/c+38/Qv9/P6L/fz8w/38/7P9/\\nP/r/fz8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/\\n/v9/P/7/fz/+/38/9v9/Px7/fz8pdX8/Cl3KPspIMDvNZX8/0Pl/P/r+fz9q/38/9P5/P+T/fz/6\\n/38/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA//v9/P/7/\\nfz/+/38//P9/P+7/fz+K/X8/qtJ9PzqLET5tf1M6L8l+P6Xwfz+2/H8/pv1/P8D8fz++/38/8v9/\\nP/7/fz8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAP/7/fz/8/38/\\n/P9/P/T/fz+C/38/UtJ/P6pMXT9g7788JymdOcqCfT+y338/VPl/P976fz/O+n8/fv9/P+b/fz/8\\n/38/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD/+/38/+v9/P/r/\\nfz/c/38/XP5/P3QZfz9Whgs/BJHyOweeHzktanQ/r2x/PyDrfz977X8/S/B/P5D9fz9G/38/wP9/\\nP+7/fz/2/38//v9/PwAAgD8AAIA/AACAPwAAgD8AAIA/AACAPwAAgD/8/38/+P9/P+D/fz/W/38/\\nUP5/P+jmfz+YtXM/FN45PoxXZjs2kg85AppRP3V0fT+Dun8/ZMl/PwzZfz90+H8/fvt/Pyj9fz8o\\n/H8/zPt/Pzj+fz/w/n8/1v9/P+b/fz/u/38/8v9/P+r/fz/u/38/0P9/P77/fz88/38/zv5/P+b0\\nfz9mNH8/bgtRP/fSrD3/PF47fMtEOZbktD7xuGY/svR7P03/fT95HH8/zcZ/P0++fz/BLX8/Det6\\nP2AMXD/mI18/tSpRP+3+eT8lUXs/Py1+PzlNfj+BGX4/tRZ/P0E6fz/3vX8/ksB/P/DZfz9qy34/\\nnDJsPweCrT6s6HQ9QYfiO9mTJjq2M5c82GQoPryrKT9Gr2Y/K1J7PyFNfz/rBX8/YA17P27ZFz88\\n/Jo9OS+IPLOH1DtVOaE8CyqoPOKQ7Dz/QAY9LWW9PDlaTD1J0gg+lAEZPyh7cz9lRX0/RMl4P8PK\\nLD+IaR8+yaQxPRytTzwJoO06OpuNOhbZUDvQraA8SAgxPt0qQz+m9Xs/6RR9Pw25bD+UUj4+QP/W\\nO3eAHjpOIZ84mFcrOAZJijdmY2I30BMqNyskTjfkxPA3sy6KOQ62DDxClAA/NK52P0yjdD8PJCM/\\nbOIXPvqAhz1JRDg9LyRJPLFWCTlfUWk5eIcQOjNtsTsXSgo+HBdePx75dj9Mx0s/wuw6PWgisjqC\\nQeA4wseFN7DWgjbI1qU1eksyNeDl8jRjQTs1edCoNS4/Qjd0gOA5Y3IDPrGsaT+v53A/aT4cP21x\\nrD0/TOg83Fu6PDDADjxVZFQ5IpxmOXhQdDk9Xm868qxDPNFNrT4kH1s/TzUZPyD67jzkSvA6taE3\\nOQE/hjgdHoE3kXajNgUtvzWVT0E1Wk8bNSk/PjWifLA2U9epOCR5pTy9XQM/Nq1OP9lw0j6VUvM8\\nniS4O2cU2ztGEaY7sZ82OTHrJjml4ss4gPNrObFgqjrmW9I8yWo6Poedhj1rOSk7ok+DOciX5Tc5\\nMn03B7SYNjQe6zWAOR41+961NMdAtTTqH+80dz9XNue1ATg1LWk7Cf3gPUp8jz7HEas9n1yDOzmC\\nQTrswoM686qJOpR0lGJ1Lg==",
                                        "language": "",
                                        "startTimestamp": 0,
                                        "endTimestamp": 0,
                                        "extensions": {},
                                        "valueCase": "text"
                                }
                            }
                    ]
            }
            while True:
                payload["timestamp"] = str(datetime.datetime.now())
                print(payload["timestamp"])
                msg = Message(json.dumps(payload))
                msg.message_id = uuid.uuid4()
                print(payload["timestamp"])
                msg.custom_properties["payload"] = "Jay"
                await module_client.send_message_to_output(msg, "outputpayload")
                print("Sent....")
                await asyncio.sleep(.1) #Sleep for 100 ms

        async def receive_message_listener(module_client):
            print("Running Receive Message")
            while True:
                input_message = await module_client.receive_message_on_input("inputpayload")  # blocking call
                #Read At
                readDateTime = datetime.datetime.now()
                message = input_message.data
                message_text = message.decode('utf-8')
                data = json.loads(message_text)

                genDateTime = datetime.datetime.strptime(data["timestamp"], '%Y-%m-%d %H:%M:%S.%f')
                diff = readDateTime - genDateTime
                diff_secs = diff.total_seconds()

                print("{}^^^^^^^{}^^^^^^^{}".format(data["timestamp"], readDateTime,diff_secs ))
                outmessage_JSON = {
                            "sent": str(data["timestamp"]),
                            "received": str(readDateTime),
                            "diffsecs": diff_secs
                }
                outmessage_text = json.dumps(outmessage_JSON)
                out_message = Message(outmessage_text)
                await module_client.send_message_to_output(input_message, "output1")

        # define behavior for receiving an input message on input1
        async def input1_listener(module_client):
            while True:
                input_message = await module_client.receive_message_on_input("input1")  # blocking call
                print("the data in the message received on input1 was ")
                print(input_message.data)
                print("custom properties are")
                print(input_message.custom_properties)
                print("forwarding mesage to output1")
                await module_client.send_message_to_output(input_message, "output1")

        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)


        # Check my role and then appropriately gentelemetry or not
        twin = await module_client.get_twin()
        print("{}".format(twin))
        if twin["desired"]["gentelemetry"] == True:
            print("Generating Telemetry")
            listeners = asyncio.gather(input1_listener(module_client), send_message(module_client),receive_message_listener(module_client))
        else:
            print("Receiving Telemetry")
            listeners = asyncio.gather(input1_listener(module_client), receive_message_listener(module_client))

        await listeners

        # Cancel listening
        listeners.cancel()

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error %s " % e )
        raise

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())
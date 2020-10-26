import socket
import sys
import time
import traceback
import queue
import json
import logging
import paho.mqtt.client as mqtt
from threading import Thread
from threading import Timer
from maxcube.connection import MaxCubeConnection
from maxcube.cube import MaxCube
from maxcube.device import MAX_DEVICE_MODE_AUTOMATIC, MAX_DEVICE_MODE_MANUAL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

class MaxcubeMqttServer:
    config = {}
    cube = None
    mqtt_client = None
    status = {}
    cube_queue = queue.Queue()
    cube_worker = None
    cube_timer = None
    connected_state = 0
    reconnect_time = 10
    error_queue = queue.Queue()

    def __init__(self, config):
        self.config = config
        self.cube_worker = Thread(target=self.cube_work)
        self.cube_worker.daemon = True
        self.cube_worker.start()

    def mqtt_connect(self):
        if self.mqtt_broker_reachable():
            logger.info('Connecting to ' + self.config['mqtt_host'] + ':' + self.config['mqtt_port'])
            self.mqtt_client = mqtt.Client(self.config['mqtt_client_id'])
            if 'mqtt_user' in self.config and 'mqtt_password' in self.config:
                self.mqtt_client.username_pw_set(self.config['mqtt_user'], self.config['mqtt_password'])

            self.mqtt_client.enable_logger()
            self.mqtt_client.on_connect = self.mqtt_on_connect
            self.mqtt_client.on_disconnect = self.mqtt_on_disconnect
            self.mqtt_client.on_message = self.mqtt_on_message
            self.connected_state = 0
            self.mqtt_client.will_set(self.config['mqtt_topic_prefix'] + "/connected", self.connected_state, 1, True)

            try:
                self.mqtt_client.connect(self.config['mqtt_host'], int(self.config['mqtt_port']), 10)
                self.mqtt_client.loop_forever()
            except:
                logger.error(traceback.format_exc())
                self.mqtt_client = None
                self.error_queue.put(1) #<-- workaround, to be tested
        else:
            logger.error(self.config['mqtt_host'] + ':' + self.config['mqtt_port'] + ' not reachable!')
            self.error_queue.put(2) #<-- workaround, to be tested

    def mqtt_on_connect(self, mqtt_client, userdata, flags, rc):
        self.connected_state = 1
        logger.info('...mqtt_connected!')
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/get/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/get/#', self.mqtt_on_message_get)
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/set/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/set/#', self.mqtt_on_message_set)
        self.mqtt_client.subscribe(self.config['mqtt_topic_prefix'] + '/command/#')
        self.mqtt_client.message_callback_add(self.config['mqtt_topic_prefix'] + '/command/#', self.mqtt_on_message_command)
        self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", self.connected_state, 1 ,True)
        self.cube_queue.put(Thread(target=self.cube_connect))

    def mqtt_on_message_get(self, client, userdata, message):
        name = message.topic.split("/")[2]
        if name in self.status:
            self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + '/status/' + name,
                                        json.dumps(self.status[name]), 0, True)
    def _set_device(self, name, data):
        dev = None
        for device in self.cube.devices:
            if device.name == name:
                dev = device
        try:
            if isinstance(data, int) or isinstance(data, float):
                logger.info('Setting device "' + name + '" target_temperature to ' + str(data))
                self.cube.set_target_temperature(dev, data)
            elif isinstance(data, dict):
                if 'val' in data and 'mode' in data:
                    logger.info('Setting device "' + name + '" target_temperature to ' + str(data))
                    self.cube.set_temperature_mode(dev, data['val'], data['mode'])
                elif 'val' in data and 'mode' not in data:
                    logger.info('Setting device "' + name + '" target_temperature to ' + str(data['val']))
                    self.cube.set_target_temperature(dev, data['val'])
                elif 'val' not in data and 'mode' in data:
                    logger.info('Setting device "' + name + '" mode to ' + str(data['mode']))
                    self.cube.set_mode(dev, data['mode'])
                else:
                    logger.warn('Got set command for device "' + name + '" with unknown structure')
            else:
                logger.warn('Got set command for device "' + name + '" with unknown structure')
        except:
            logger.error(traceback.format_exc())
            self.error_queue.put(6) #<-- workaround, to be tested
    def mqtt_on_message_set(self, client, userdata, message):
        name = message.topic.split("/")[2]
        if name in self.status:
            try:
                data = json.loads(message.payload)
                self.cube_queue.put(Thread(target=self._set_device, args=(name, data)))
            except:
                logger.error(traceback.format_exc())
                self.error_queue.put(5) #<-- workaround, to be tested
        else:
            logger.warn('Got set command for unknown device "' + name+ '"')

    def mqtt_on_message_command(self, client, userdata, message):
        pass

    def mqtt_on_disconnect(self, mqtt_client, userdata, rc):
        self.connected_state = 0
        logger.info('Diconnected! will reconnect! ...')
        if rc is 0:
            self.mqtt_connect()
        else:
            time.sleep(5)
            while not self.mqtt_broker_reachable():
                time.sleep(10)
            self.mqtt_client.reconnect()

    def mqtt_on_message(self, client, userdata, message):
        logger.warn('Unhandled message to "' + message.topic + '"')

    def mqtt_broker_reachable(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        try:
            s.connect((self.config['mqtt_host'], int(self.config['mqtt_port'])))
            s.close()
            return True
        except socket.error:
            logger.error(traceback.format_exc())
            self.error_queue.put(7) #<-- workaround, to be tested


    def cube_work(self):
        while True:
            if not self.cube_queue.empty():
                job = self.cube_queue.get()
                job.start()
                job.join()
                self.cube_queue.task_done()
            time.sleep(0.5)

    def cube_connect(self):
        logger.info('Connecting to maxcube on "'+ self.config['cube_host'] + '"...')
        try:
            self.cube = MaxCube(MaxCubeConnection(self.config['cube_host'], int(self.config['cube_port'])))
        except socket.error:
            logger.error("MaxCube not reachable")
            self.connected_state = 1
            self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", self.connected_state, 1 ,True)
            self.error_queue.put(3) #<-- workaround, to be tested
            self.cube_queue.put(Thread(target=time.sleep, args=(self.reconnect_time,)))
            self.cube_queue.put(Thread(target=self.cube_connect))
            return

        self.connected_state = 2
        self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", self.connected_state, 1, True)

        logger.info('...cube connected!')

        if self.cube_timer:
            self.cube_timer.cancel()

        self.update_cube()

    def update_cube(self):
        logger.debug
        ('Cube update')
        try:
            self.cube.update()
        except socket.error:
            logger.error("MaxCube not reachable")
            self.error_queue.put(4) #<-- workaround, to be tested
            self.connected_state = 1
            self.mqtt_client.publish(self.config['mqtt_topic_prefix'] + "/connected", self.connected_state, 1 ,True)
            self.cube_queue.put(Thread(target=time.sleep, args=(self.reconnect_time,)))
            self.cube_queue.put(Thread(target=self.update_cube))
            return
        except:
            logger.error(traceback.format_exc())
            self.error_queue.put(8) #<-- workaround, to be tested
        self.publish_status()
        self.cube_timer = Timer(60, self.update_cube)
        self.cube_timer.daemon = True
        self.cube_timer.start()

    def _update_device(self,device):
        changed=False
        # Timestamp in ms since epoch
        ts = int(time.time()*1000)

        if MaxCube.is_thermostat(device) or MaxCube.is_wallthermostat(device):
            if device.name not in self.status:
                #logger.info('New thermostat: "' + device.name)
                self.status[device.name] = { 'val': -1,
                                             'target_temperature': -1,
                                             'lc': ts,
                                             'ts': ts,
                                             'serial': device.serial,
                                             'mode': -1 }

            if (self.status[device.name]['val'] != device.actual_temperature
                or self.status[device.name]['target_temperature'] != device.target_temperature
                or self.status[device.name]['mode'] != device.mode):
                self.status[device.name]['lc'] = self.status[device.name]['ts']
                self.status[device.name]['ts'] = ts
                self.status[device.name]['val'] = device.actual_temperature
                self.status[device.name]['target_temperature'] = device.target_temperature
                self.status[device.name]['mode'] = device.mode
                changed = True

        elif MaxCube.is_windowshutter(device):
            if device.name not in self.status:
                #logger.info('New windowshutter: "' + device.name)
                self.status[device.name] = { 'val': device.is_open,
                                             'lc': ts,
                                             'ts': ts,
                                             'serial': device.serial}
                changed = True

            elif self.status[device.name]['val'] != device.is_open:
                self.status[device.name]['lc'] = self.status[device.name]['ts']
                self.status[device.name]['ts'] = ts
                self.status[device.name]['val'] = device.is_open

        return changed

    def publish_status_single(name):
        ''' publishes the state of the device with the given name'''
        topic_prefix = self.config['mqtt_topic_prefix'] + '/status/'
        for device in self.cube.devices:
            if device.name == name:
                self._update_device(device)
                self.mqtt_client.publish(topic_prefix + device.name,
                                        json.dumps(self.config[device.name]), 0, True)

    def publish_status(self):
        ''' publishes the state of every device with a state change'''
        topic_prefix = self.config['mqtt_topic_prefix'] + '/status/'
        for device in self.cube.devices:
            if self._update_device(device):
                self.mqtt_client.publish(topic_prefix + device.name,
                                        json.dumps(self.status[device.name]), 0, True)

    def start(self):
        self.mainloop = Thread(target=self.mqtt_connect)
        self.mainloop.daemon = True
        self.mainloop.start()
        while True:
            time.sleep(0.5)
            if not self.error_queue.empty():
                rc = self.error_queue.get()
                logger.info('sys.exit(%i)' % rc)
                sys.exit(rc)
        logger.info('sys.exit(99)')
        sys.exit(99)

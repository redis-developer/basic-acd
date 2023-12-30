import requests
from argparse import ArgumentParser
from faker import Faker
from faker.providers import DynamicProvider
import os
from dotenv import load_dotenv
from states import ACD_STATE, AGENT_STATE, CONTACT_STATE
import time
import logging
import threading

NUM_AGENTS: int = 40
NUM_CONTACTS: int = 100

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('simulator')
logger.setLevel(logging.INFO)
logger.info('Simulator started')

language_provider = DynamicProvider(
    provider_name='language',
    elements=['English', 'Spanish']
)

expertise_provider = DynamicProvider(
    provider_name='expertise',
    elements=['Support', 'Disputes', 'Billing']
)

Faker.seed(0)
fake: Faker = Faker()
fake.add_provider(language_provider)
fake.add_provider(expertise_provider) 

def openAcd(rest_url: str, agents: int) -> None:
    """ 
        Creates an ACD environment with agents each having a set of skills and then 
        sets the ACD to the open state.
        
        Parameters
        ----------
        rest_url - Base url of the REST API server
        agents - Number of agents to create.  Agents have random names and skills.
 
        Returns
        -------
        None
    """
    for i in range(agents):
        payload: dict = {
            'fname': fake.first_name(),
            'lname': fake.last_name(),
            'skills': [fake.language(), fake.expertise()]
        }
        requests.post(f'{rest_url}/agent/agent:{i}', json=payload)
    
    payload = { 
        'state': ACD_STATE.OPEN.value  
    }
    requests.post(f'{rest_url}/acd', json=payload)

def generate(rest_url: str) -> None:
    """ 
        Contact generator.  Performs the following flow:
            - Creates 1 contact with random skill requirements.
            - Allows the contact to have duration (sleep) for a random amount of time
            - Completes the contact:
                - if the contact made it out of queue to an agent, that agent is set Available again
                - if not, the contact is considered 'abandoned', i.e. the caller hung up while it was in queue
        
        Parameters
        ----------
        rest_url - Base url of the REST API server
 
        Returns
        -------
        None
    """
    payload = { 'skills': [fake.language(), fake.expertise()] }
    response = requests.post(f'{rest_url}/contact', json=payload)  # generate contact
    if response.ok:
        contact_key = response.json()['contact_key']
        time.sleep(fake.pyfloat(min_value=1,max_value=3))
        response = requests.get(f'{rest_url}/contact/{contact_key}')  # get contact status
        if response.ok:
            if CONTACT_STATE(response.json()['state']) == CONTACT_STATE.ASSIGNED:
                logger.info(f'{contact_key} complete with {response.json()["agent"]}')
                payload = { 'state': AGENT_STATE.AVAILABLE.value }
                requests.patch(f'{rest_url}/agent/{response.json()["agent"]}/state', json=payload)
            else:
                logger.info(f'{contact_key} abandoned')
    requests.patch(f'{rest_url}/contact/{contact_key}')  # complete contact
        
def closeAcd(rest_url: str) -> None:
    """ 
        Sets the ACD to the closed state.
        
        Parameters
        ----------
        rest_url - Base url of the REST API server
        
        Returns
        -------
        None
    """    
    payload = { 
        'state': ACD_STATE.CLOSED.value  
    }
    requests.post(f'{rest_url}/acd', json=payload)

if __name__ == '__main__':
    parser = ArgumentParser(description='Basic ACD simulator')
    parser.add_argument('--agents', required=False, type=int, default=NUM_AGENTS,
        help='Number of Agents in Simulation')
    parser.add_argument('--contacts', required=False, type=int, default=NUM_CONTACTS,
        help='Number of Contacts in Simulation')
    args = parser.parse_args()

    load_dotenv(override=True)
    rest_url: str = os.getenv('REST_URL')
    openAcd(rest_url, args.agents)

    for _ in range(args.contacts):
        thread = threading.Thread(target=generate, args=(rest_url,))
        thread.start()
        time.sleep(.5)  
    time.sleep(3)

    closeAcd(rest_url)
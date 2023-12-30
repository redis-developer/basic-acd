import asyncio
from redis import asyncio as aioredis
from redis import Redis
import operations as ops
from states import AGENT_STATE, CONTACT_STATE
from dotenv import load_dotenv
import os
from response import Response, RESPONSE_TYPE
from random import uniform
import logging

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger('dispatcher')
logger.setLevel(logging.INFO)
logger.info('Dispatcher started')

async def dispatch(client: Redis) -> None:
    """ 
        Monitors a FIFO queue for new contacts and then routes them to an agent.  
            - Queue is realized via a Redis Sorted Set.  Members of that set are contacts and the scores are timestamps
                of entry to queue.
            - Contacts have multiple skill requirements
            - Agents have multiple skills. 
            - Available agents are stored in Sorted Sets, 1 set per skill.  Scoring is a timestamp of when the agent is available.
            - Matching of contacts and agents is done with a Sorted Set intersection of skills.  This obtains the longest available
                agent (LAA) with the requisite skills.
        
        Parameters
        ----------
        client - Redis asyncio client
 
        Returns
        -------
        None
    """
    while True:
        try:
            response: list[tuple] = await client.bzpopmin('queue') # using a sorted set as a fifo queue
            contact_key: str = response[1].decode('utf-8')
            timestamp: int = int(response[2])
            skills: list[list[str]] = await client.json().get(contact_key, '$.skills')
            avail_keys: list[str] = [f'{{availAgentsSkill}}:{skill}' for skill in skills[0]]
            agents: list[str]  = await client.zinter(avail_keys)
            agents = [agent.decode('utf-8') for agent in agents]
            found: bool = False
            for agent in agents:
                response: Response = await ops.set_agent_state(client, agent, AGENT_STATE.UNAVAILABLE)
                if response.resp_type == RESPONSE_TYPE.OK:
                    found = True
                    await client.json().mset([(contact_key, '$.agent', agent), 
                                          (contact_key, '$.state', CONTACT_STATE.ASSIGNED.value)])
                    logger.info(f'{contact_key} assigned to {agent}')
                    break
  
            if not found:
                # check if the contact has been abandoned
                state: list[int] = (await client.json().get(contact_key, '$.state'))[0]
                if CONTACT_STATE(state) != CONTACT_STATE.COMPLETE:
                    # no agent avail.  put contact back on queue with a 1 sec decelerator to allow other contacts to bubble up
                    await client.zadd('queue', mapping={ contact_key: timestamp+1000 }) 
                    logger.info(f'{contact_key} queued')
                    await asyncio.sleep(uniform(0, 2))
        except Exception as err:
            logger.error(err)

if __name__ == '__main__':
    load_dotenv(override=True)
    client:Redis = aioredis.from_url(os.getenv('REDIS_URL'))
    asyncio.run(dispatch(client))             
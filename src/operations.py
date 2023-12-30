import time
from redis.asyncio.lock import Lock
from redis import Redis
from response import Response, RESPONSE_TYPE
from states import ACD_STATE, AGENT_STATE, CONTACT_STATE
from uuid import uuid4
import json

LOCK_TIMEOUT = 1 # 1 sec
BLOCK_TIME = .1  # 100 ms
CONTACT_TTL = 60*60 #3600 sec/1 hr

async def set_acd_state(client: Redis,
                        acd_state: ACD_STATE) -> Response:
    """ 
        Operation for opening/closing ACD.
        
        Parameters
        ----------
        client - Redis asyncio client
        acd_state - state to be applied (open or close)
 
        Returns
        -------
        Response - object containing status and result
    """
    resp_type: RESPONSE_TYPE = None
    result: str = None
    agent_state: AGENT_STATE = None
 
    try:
        match acd_state:
            case ACD_STATE.OPEN:
                agent_state = AGENT_STATE.AVAILABLE
            case ACD_STATE.CLOSED:
                agent_state = AGENT_STATE.UNAVAILABLE
            case _:
                result = f'set_acd_state - invalid acd state'
                resp_type = RESPONSE_TYPE.ERR
        
        if agent_state:
            async for agent_key in client.scan_iter(f'agent:*'):
                await set_agent_state(client, agent_key, agent_state)
            resp_type = RESPONSE_TYPE.OK  
            result = acd_state.value      
    except Exception as err:
        result = f'set_acd_state - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)
       
async def create_contact(client: Redis,
                         skills: list[str]) -> Response:
    """ 
        Operation for creating a new contact.
        
        Parameters
        ----------
        client - Redis asyncio client
        skills - array of skills required for the contact
 
        Returns
        -------
        Response - object containing status and result
    """    
    resp_type: RESPONSE_TYPE = None
    result: str = None
    contact_key: str = f'contact:{str(uuid4())}'
    try:
        await client.json().set(contact_key, '$', {'skills': skills, 'state': CONTACT_STATE.QUEUED.value, 'agent': None})
        await client.zadd('queue', mapping={ contact_key: round(time.time()*1000) })  #time in ms
        resp_type = RESPONSE_TYPE.OK
        result = contact_key
    except Exception as err:
        result = f'create_contact - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)
    
async def complete_contact(client: Redis,
                           contact_key: str) -> Response:
    """ 
        Operation for completing a given contact.  Sets the state attribute in the associated Redis JSON
        object and sets that object to expire.
        
        Parameters
        ----------
        client - Redis asyncio client
        contact_key - Redis key for contact.   Value of the key is a JSON object.
 
        Returns
        -------
        Response - object containing status and result
    """    
    resp_type: RESPONSE_TYPE = None
    result: str = None
    try:
        pipe: Redis = await client.pipeline(transaction=True)
        await pipe.json().set(contact_key, '$.state', CONTACT_STATE.COMPLETE.value)
        await pipe.expire(contact_key, CONTACT_TTL)
        await pipe.execute()
        resp_type = RESPONSE_TYPE.OK
        result = contact_key
    except Exception as err:
        result = f'complete_contact - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)

async def get_contact(client: Redis,
                      contact_key: str) -> Response:
    """ 
        Operation for fetching a given contact
        
        Parameters
        ----------
        client - Redis asyncio client
        contact_key - Redis key for contact.   Value of the key is a JSON object.
 
        Returns
        -------
        Response - object containing status and result
    """   
    resp_type: RESPONSE_TYPE = None
    result: str = None
    try:
        contact: dict = await client.json().get(contact_key)
        print(f'contact: {contact}')
        if contact:
            result = json.dumps(contact)
            resp_type = RESPONSE_TYPE.OK
        else:
            result = f'get_contact - {contact_key} does not exist'
            resp_type = RESPONSE_TYPE.ERR
    except Exception as err:
        result = f'get_contact - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)
    
async def create_agent(client: Redis, 
                       agent_key: str, 
                       fname: str,
                       lname: str,
                       skills: list[str]) -> Response:
    """ 
        Operation for creating an agent
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        fname - first name of agent.
        lname - last name of agent.
        skills - array of skills the agent possesses
 
        Returns
        -------
        Response - object containing status and result
    """   
    resp_type: RESPONSE_TYPE = None
    result: str = None
    try:
        lock: Lock = Lock(redis=client, name=f'{agent_key}:lock', timeout=LOCK_TIMEOUT, blocking_timeout=BLOCK_TIME)
        lock_acquired: bool = await lock.acquire()
        if lock_acquired:
            exists: int = await client.exists(agent_key)
            if exists:
                result = f'create_agent - agent {agent_key} already exists'
                resp_type = RESPONSE_TYPE.ERR
            else:
                agent_obj: dict = { 'id': agent_key, 'fname': fname, 'lname': lname, 'skills': skills, 'state': AGENT_STATE.UNAVAILABLE.value }
                await client.json().set(agent_key, '$', agent_obj)
                result = agent_key
                resp_type = RESPONSE_TYPE.OK
        else:
            resp_type = RESPONSE_TYPE.LOCKED
    except Exception as err:
        result = f'create_agent - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        if await lock.locked():
            await lock.release()
        return Response(resp_type, result)

async def delete_agent(client: Redis,  
                       agent_key: str) -> Response:
    """ 
        Operation for deleting an agent
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
 
        Returns
        -------
        Response - object containing status and result
    """   
    resp_type: RESPONSE_TYPE = None
    result: str = None

    try:
        lock: Lock = Lock(redis=client, name=f'{agent_key}:lock', timeout=LOCK_TIMEOUT, blocking_timeout=BLOCK_TIME)
        lock_acquired: bool = await lock.acquire()
        if lock_acquired:
            exists: int = await client.exists(agent_key)
            if not exists:
                result = f'delete_agent - agent {agent_key} does not exist'
                resp_type = RESPONSE_TYPE.ERR
            else:
                skills: list[list[str]] = await client.json().get(agent_key, '$.skills')    
                for skill in skills[0]:
                    await client.zrem(f'{{availAgentsSkill}}:{skill}', agent_key)
                await client.json().delete(agent_key)
                resp_type = RESPONSE_TYPE.OK
                result = agent_key
        else:
            resp_type = RESPONSE_TYPE.LOCKED
    except Exception as err:
        result = f'delete_agent - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        if await lock.locked():
            await lock.release()
        return Response(resp_type, result)
    
async def set_agent_state(client: Redis, 
                          agent_key: str,
                          state: AGENT_STATE) -> Response:
    """ 
        Operation for changing an agent's state.  An agent can be in an available or unavailable state.
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        state - new state
 
        Returns
        -------
        Response - object containing status and result
    """   
    resp_type: RESPONSE_TYPE = None
    result: str = None

    try:
        lock: Lock = Lock(redis=client, name=f'{agent_key}:lock', timeout=LOCK_TIMEOUT, blocking_timeout=BLOCK_TIME)
        lock_acquired: bool = await lock.acquire()
        if lock_acquired:
            exists: int = await client.exists(agent_key)
            if not exists:
                result = f'set_agent_state - {agent_key} does not exist'
                resp_type = RESPONSE_TYPE.ERR
            else:
                current_state = (await client.json().get(agent_key, '$.state'))[0]
                if AGENT_STATE(current_state) != state: 
                    skills: list[list[str]] = await client.json().get(agent_key, '$.skills')    
                    for skill in skills[0]:
                        match state:
                            case AGENT_STATE.AVAILABLE: 
                                await client.zadd(f'{{availAgentsSkill}}:{skill}', mapping={ agent_key: round(time.time()*1000) })
                                await client.json().set(agent_key, '$.state', AGENT_STATE.AVAILABLE.value)  
                            case AGENT_STATE.UNAVAILABLE:
                                await client.zrem(f'{{availAgentsSkill}}:{skill}', agent_key)
                                await client.json().set(agent_key, '$.state', AGENT_STATE.UNAVAILABLE.value)  
                            case _:
                                raise Exception(f'invalid agent state parameter: {state}') 
                    result = agent_key
                    resp_type = RESPONSE_TYPE.OK
                else:
                    result = f'set_agent_state - {agent_key} already in {AGENT_STATE(current_state)}'
                    resp_type = RESPONSE_TYPE.ERR
        else:
            resp_type = RESPONSE_TYPE.LOCKED
    except Exception as err:
        result = f'set_agent_state - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        if await lock.locked():
            await lock.release()
        return Response(resp_type, result)

async def change_agent_info(client: Redis, 
                            agent_key: str, 
                            fname: str,
                            lname: str) -> Response:
    """ 
        Operation for changing an agent's info: first name, last name.
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        fname - first name
        lname - last name
 
        Returns
        -------
        Response - object containing status and result
    """
    resp_type: RESPONSE_TYPE = None
    result: str = None

    try:
        exists: int = await client.exists(agent_key)
        if not exists:
            result = f'change_agent_info - {agent_key} does not exist'
            resp_type = RESPONSE_TYPE.ERR
        else:
            await client.json().mset([(agent_key, '$.fname', fname),  (agent_key, '$.lname', lname)])
            result = agent_key
            resp_type = RESPONSE_TYPE.OK
    except Exception as err:
        result = f'change_agent_info - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)

async def add_agent_skill(client: Redis,
                          agent_key: str,
                          skill: str) -> Response:
    """ 
        Operation for adding a skill to an agent.  Availability queues are implemented via Redis Sorted Sets.
        The members of a given set are agents with the associated skill.  The score is a timestamp (ms) when the agent
        became available.  Availability Sorted Sets are stored via hash tag to ensure they are all on the same shard.  This
        allows subsequent intersection queries (ZINTER) to obtain the longest available agent with the requisite skills.
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        skill - skill to be added.  Each agent JSON object has an array of skills.
 
        Returns
        -------
        Response - object containing status and result
    """
    resp_type: RESPONSE_TYPE = None
    result: str = None 

    try:
        lock: Lock = Lock(redis=client, name=f'{agent_key}:lock', timeout=LOCK_TIMEOUT, blocking_timeout=BLOCK_TIME)
        lock_acquired: bool = await lock.acquire()
        if lock_acquired:
            exists: int = await client.exists(agent_key)
            if not exists:
                result = f'add_agent_skill - {agent_key} does not exist'
                resp_type = RESPONSE_TYPE.ERR
            else:
                await client.json().arrappend(agent_key, '$.skills', skill)
                current_state = (await client.json().get(agent_key, '$.state'))[0]
                if AGENT_STATE(current_state) == AGENT_STATE.AVAILABLE:
                    await client.zadd(f'{{availAgentsSkill}}:{skill}', mapping={ agent_key: round(time.time()*1000) })
                result = agent_key
                resp_type = RESPONSE_TYPE.OK
        else:
            resp_type = RESPONSE_TYPE.LOCKED
    except Exception as err:
        result = f'add_agent_skill - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        if await lock.locked():
            await lock.release()
        return Response(resp_type, result)

async def delete_agent_skill(client: Redis,
                             agent_key: str,
                             skill: str) -> Response:
    """ 
        Operation for deleting a skill from an agent. Has the side effect of removing the agent from the associated
        skill availability queue.
        
        Parameters
        ----------
        client - Redis asyncio client
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        skill - skill to be deleted.
 
        Returns
        -------
        Response - object containing status and result
    """
    resp_type: RESPONSE_TYPE = None
    result: str = None 

    try:
        lock: Lock = Lock(redis=client, name=f'{agent_key}:lock', timeout=LOCK_TIMEOUT, blocking_timeout=BLOCK_TIME)
        lock_acquired: bool = await lock.acquire()
        if lock_acquired:
            exists: int = await client.exists(agent_key)
            if not exists:
                result = f'delete_agent_skill - {agent_key} does not exist'
                resp_type = RESPONSE_TYPE.ERR
            else:
                idx: int = (await client.json().arrindex(agent_key, '$.skills', skill))[0]
                if idx >= 0:
                    await client.json().arrpop(agent_key, '$.skills', idx)
                    await client.zrem(f'{{availAgentsSkill}}:{skill}', agent_key)
                    result = agent_key
                    resp_type = RESPONSE_TYPE.OK
                else:
                    resp_type = RESPONSE_TYPE.ERR
                    result = f'delete_agent_skill - agent does not have skill {skill}'
        else:
            resp_type = RESPONSE_TYPE.LOCKED
    except Exception as err:
        result = f'delete_agent_skill - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        if await lock.locked():
            await lock.release()
        return Response(resp_type, result)

async def delete_skill(client: Redis,
                       skill: str) -> Response:
    """ 
        Operation for deleting a skill entirely.  Has the side effect of removing the skill from any associated agents 
        as well.
        
        Parameters
        ----------
        client - Redis asyncio client
        skill - skill to be deleted.
 
        Returns
        -------
        Response - object containing status and result
    """
    resp_type: RESPONSE_TYPE = None
    result: str = None 
    try:
        await client.delete(f'{{availAgentsSkill}}:{skill}')
        async for agent_key in client.scan_iter('agent:*'):
            await delete_agent_skill(client, agent_key, skill)
        result = skill
        resp_type = RESPONSE_TYPE.OK
    except Exception as err:
        result = f'delete_skill - {err}'
        resp_type = RESPONSE_TYPE.ERR
    finally:
        return Response(resp_type, result)
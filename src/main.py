import operations as ops
from response import Response, RESPONSE_TYPE
from states import ACD_STATE, AGENT_STATE
from fastapi import FastAPI, HTTPException, status, Body, Path
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
from redis import asyncio as aioredis
from redis import Redis
from typing import Annotated
import json

client: Redis = None

@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    """ 
        FastAPI start up and shut down procedures.  Initializes the Redis asyncio client.
        
        Parameters
        ----------
        app - FastAPI app
 
        Returns
        -------
        None
    """
    load_dotenv(override=True)
    global client
    client = aioredis.from_url(os.getenv('REDIS_URL'))
    yield
    await client.quit()

app = FastAPI(lifespan=lifespan)

@app.post('/acd', status_code=status.HTTP_201_CREATED)
async def set_acd_state(state: Annotated[int, Body(embed=True)]) -> None:
    """ 
        Route for opening/closing ACD.
        
        Parameters
        ----------
        state - body param indicating state to be applied (open = 1, close = 0)
 
        Returns
        -------
        None
    """
    response: Response = await ops.set_acd_state(client, ACD_STATE(state))
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'acd_state': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.post('/contact', status_code=status.HTTP_201_CREATED)
async def create_contact(skills: Annotated[list[str], Body(embed=True)]) -> None:
    """ 
        Route for creating a new contact.
        
        Parameters
        ----------
        skills - body param with array of skills required for the contact
 
        Returns
        -------
        None
    """    
    response: Response = await ops.create_contact(client, skills) 
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'contact_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.patch('/contact/{contact_key}', status_code=status.HTTP_200_OK)
async def complete_contact(contact_key: Annotated[str, Path()]) -> None:
    """ 
        Route for completing a given contact
        
        Parameters
        ----------
        contact_key - Redis key for contact.   Value of the key is a JSON object.
 
        Returns
        -------
        None
    """    
    response: Response = await ops.complete_contact(client, contact_key)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'contact_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.get('/contact/{contact_key}', status_code=status.HTTP_200_OK)
async def get_contact(contact_key: Annotated[str, Path()]) -> None:
    """ 
        Route for fetching a given contact
        
        Parameters
        ----------
        contact_key - Redis key for contact.   Value of the key is a JSON object.
 
        Returns
        -------
        None
    """   
    response: Response = await ops.get_contact(client, contact_key)
    if response.resp_type == RESPONSE_TYPE.OK:
        return json.loads(response.result)
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)
          
@app.post('/agent/{agent_key}', status_code=status.HTTP_201_CREATED)
async def create_agent(agent_key: Annotated[str, Path()],
                       fname: Annotated[str, Body(embed=True)],
                       lname: Annotated[str, Body(embed=True)],
                       skills: Annotated[list[str], Body(embed=True)]) -> None:
    """ 
        Route for creating an agent
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        fname - first name of agent.
        lname - last name of agent.
        skills - array of skills the agent possesses
 
        Returns
        -------
        None
    """   
    response: Response = await ops.create_agent(client, agent_key, fname, lname, skills) 
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'agent_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.delete('/agent/{agent_key}', status_code=status.HTTP_200_OK)
async def delete_agent(agent_key: Annotated[str, Path()]) -> None:
    """ 
        Route for deleting an agent
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
 
        Returns
        -------
        None
    """   
    response: Response = await ops.delete_agent(client, agent_key)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'agent_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.patch('/agent/{agent_key}/state', status_code=status.HTTP_200_OK)
async def set_agent_state(agent_key: Annotated[str, Path()],
                          state: Annotated[int, Body(embed=True)]) -> None:
    """ 
        Route for changing an agent's state.  An agent can be in an available or unavailable state.
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        state - new state
 
        Returns
        -------
        None
    """   
    response: Response = await ops.set_agent_state(client, agent_key, AGENT_STATE(state))
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'agent_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.patch('/agent/{agent_key}', status_code=status.HTTP_200_OK)
async def change_agent_info(agent_key: Annotated[str, Path()],
                            fname: Annotated[str, Body(embed=True)],
                            lname: Annotated[str, Body(embed=True)]) -> None:
    """ 
        Route for changing an agent's info: first name, last name.
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        fname - first name
        lname - last name
 
        Returns
        -------
        None
    """
    response: Response = await ops.change_agent_info(client, agent_key, fname, lname)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'agent_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)

@app.patch('/agent/{agent_key}/skill', status_code=status.HTTP_200_OK)
async def add_agent_skill(agent_key: Annotated[str, Path()],
                          skill: Annotated[str, Body(embed=True)]) -> None:
    """ 
        Route for adding a skill to an agent.
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        skill - skill to be added.  Each agent JSON object has an array of skills.
 
        Returns
        -------
        None
    """
    response: Response = await ops.add_agent_skill(client, agent_key, skill)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'agent_key': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)
    
@app.delete('/agent/{agent_key}/skill/{skill}', status_code=status.HTTP_200_OK)
async def delete_agent_skill(agent_key: Annotated[str, Path()], 
                             skill: Annotated[str, Path()]) -> None:
    """ 
        Route for deleting a skill from an agent. Has the side effect of removing the agent from the associated
        skill availability queue.
        
        Parameters
        ----------
        agent_key - Redis key for agent.   Value of the key is a JSON object.
        skill - skill to be deleted.
 
        Returns
        -------
        None
    """
    response: Response = await ops.delete_agent_skill(client, agent_key, skill)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'skill': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)
    
@app.delete('/skill/{skill}', status_code=status.HTTP_200_OK)
async def delete_skill(skill: Annotated[str, Path()]) -> None:
    """ 
        Route for deleting a skill entirely.  Has the side effect of removing the skill from any associated agents 
        as well.
        
        Parameters
        ----------
        skill - skill to be deleted.
 
        Returns
        -------
        None
    """
    response: Response = await ops.delete_skill(client, skill)
    if response.resp_type == RESPONSE_TYPE.OK:
        return {'skill': response.result}
    else:
        raise HTTPException(status_code=response.resp_type, detail=response.result)
/*
 * Copyright (c) vispractice
 * 2013.
 */
package com.vispractice.vcsb.neo4j;

import java.net.URI;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import javax.ws.rs.core.MediaType;

import com.google.gson.JsonObject;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * @author liwei
 * 
 */
public class Neo4jClient extends DB {
	private String basePointUri = null;
	private String nodePointUri = null;
	private String cypherPointUri = null;
	private final String QUERY = "{\"query\":\"START n=node(1) WHERE n._key = {key} and n._table = {table} RETURN n\","
			+ "\"params\":{\"key\":\"%s\",\"table\":\"%s\"}}";

	@Override
    public void init() throws DBException {
		Properties props = getProperties();
		basePointUri = props.getProperty("neo4j.url","http://localhost:7474/db/data");
		nodePointUri = basePointUri + "/node";
		cypherPointUri = basePointUri + "/cypher";
	}
	
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		
		WebResource resource = Client.create().resource(cypherPointUri);
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(String.format(QUERY, key,table,key,table))
				.post(ClientResponse.class);
//		if (fields != null) {
//            Iterator<String> iter = fields.iterator();
//            while (iter.hasNext()) {
//            }
//        }
		
		System.out.println(response.getEntity( String.class ));
		
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		return 0;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		WebResource resource = Client.create().resource(nodePointUri);

		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity("{}")
				.post(ClientResponse.class);

		final URI location = response.getLocation();
		final String nodeUri = location.toString();
		System.out.println(String.format(
				"POST to [%s], status code [%d], location header [%s]",
				nodePointUri, response.getStatus(), nodeUri));
		response.close();

		JsonObject jo = new JsonObject();
		jo.addProperty("_key", key);
		jo.addProperty("_table", table);
		for (String k : values.keySet()) {
			jo.addProperty(k, values.get(k).toString());
		}

		resource = Client.create().resource(nodeUri + "/properties");
		response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(jo.toString())
				.put(ClientResponse.class);

		System.out.println(String.format("PUT to [%s], status code [%d]",
				nodeUri, response.getStatus()));
		response.close();

		return 0;
	}

	@Override
	public int delete(String table, String key) {
		return 0;
	}

	public static void main(String[] args) {
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

		for (int i = 2; i < 1*100; i++) {
			values.put("p"+i, new StringByteIterator(UUID.randomUUID().toString()));
		}
		Neo4jClient nc = new Neo4jClient();
		try {
			nc.init();
		} catch (DBException e) {
			e.printStackTrace(System.err);
		}
		//nc.insert("test1", "key1", values);
		nc.read("test1", "key1", null, null);
	}
}

/*
 * Copyright (c) vispractice
 * 2013.
 */
package com.vispractice.vcsb.neo4j;

import java.net.URI;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;

/**
 * @author liwei
 * 
 */
public class Neo4jClient extends DB {
	private final String nodeEntryPointUri = "http://localhost:7474/db/data/node";

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
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
		WebResource resource = Client.create().resource(nodeEntryPointUri);
		
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity("{}")
				.post(ClientResponse.class);

		final URI location = response.getLocation();
		System.out.println(String.format(
				"POST to [%s], status code [%d], location header [%s]",
				nodeEntryPointUri, response.getStatus(), location.toString()));
		response.close();
		
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		return 0;
	}

}

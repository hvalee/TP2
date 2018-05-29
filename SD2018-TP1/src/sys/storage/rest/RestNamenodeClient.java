package sys.storage.rest;

import static sys.storage.rest.namenode.NamenodeServer.NAMENODE;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import api.storage.Namenode;
import discovery.Discover;

public class RestNamenodeClient extends RestClient implements Namenode {
	
	public RestNamenodeClient() {
		this( Discover.uriOf(NAMENODE) );
	}

	public RestNamenodeClient( URI namenode ) {
		super( namenode, Namenode.PATH );
	}

	@Override
	public List<String> list(String prefix ) {		
		return reTry( () -> _list( prefix) );
	}
	
	@Override
	public List<String> read(String name) {		
		return reTry( () -> _read( name ) );	
	}
	
	@Override
	public void create(String name, List<String> metadata) {
		reTry( () -> _create(name, metadata));
	}
		
	@Override
	public void update(String name, List<String> metadata) {
		reTry( () -> _update(name, metadata));
	}
	
	@Override
	public void delete(String prefix) {
		reTry( () -> _delete(prefix));
	}

	private List<String> _list(String prefix ) {		
		Response r = target.path("list")
			.queryParam("prefix", prefix)
			.request()
			.accept(MediaType.APPLICATION_JSON)
			.get();
		return responseContents( Status.OK, r, Collections.emptyList(), new GenericType<List<String>>() {});
	}

	private List<String> _read(String name) {
		Response r = target.path(name)
				.request()
				.accept(MediaType.APPLICATION_JSON)
				.get();
		return responseContents( Status.OK, r, null, new GenericType<List<String>>() {});
	}

	private void _create(String name, List<String> metadata) {
		Response r = target.path(name)
				.request()
				.post( Entity.entity( metadata, MediaType.APPLICATION_JSON));
		
		verifyResponse( Status.NO_CONTENT, r);
	}

	private void _update(String name, List<String> metadata) {
		Response r = target.path(name)
				.request()
				.put( Entity.entity( metadata, MediaType.APPLICATION_JSON));
		
		verifyResponse( Status.NO_CONTENT, r);
	}

	private void _delete(String prefix) {
		Response r = target.path("list")
				.queryParam("prefix", prefix)
				.request()
				.delete();		
		verifyResponse( Status.NO_CONTENT, r);
	}
}

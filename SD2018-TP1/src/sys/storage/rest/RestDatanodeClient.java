package sys.storage.rest;

import java.net.URI;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import api.storage.Datanode;

public class RestDatanodeClient extends RestClient implements Datanode {

	public RestDatanodeClient(URI datanode) {
		super(datanode, Datanode.PATH );
	}

	@Override
	public String createBlock(byte[] data, String blob) {
		return reTry( () -> _createBlock(data, blob));
	}
	
	@Override
	public void deleteBlock(String block) {
		reTry( () -> _deleteBlock(block));
	}
		
	@Override
	public byte[] readBlock(String block) {
		return reTry( () -> _readBlock(block));
	}
	
	//For GC, add the blob name as an optional query parameter, keeping compatibility with the original API. 
	private String _createBlock(byte[] data, String blob) {
		Response r = target
				.queryParam("blob", blob)
				.request()
				.accept(MediaType.APPLICATION_JSON)
				.post(Entity.entity(data, MediaType.APPLICATION_OCTET_STREAM));

		return responseContents(Status.OK, r, "<failed>", new GenericType<String>(){});
	}

	private void _deleteBlock(String block) {
		Response r = client.target(block)
				.request()
				.delete();

		verifyResponse(Status.NO_CONTENT, r);
	}

	private byte[] _readBlock(String block) {
		Response r = client.target(block)
				.request()
				.accept(MediaType.APPLICATION_OCTET_STREAM)
				.get();

		return responseContents(Status.OK, r, new byte[0], new GenericType<byte[]>(){});
	}
}
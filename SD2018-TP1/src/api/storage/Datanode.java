package api.storage;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path( Datanode.PATH ) 
public interface Datanode {

	static final String PATH = "/datanode";
	
	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	String createBlock(byte[] data, @QueryParam("blob") String blob);
	//Note the blob parameter is optional and is used for implementing the garbage collection
	//It tells the datanode with blob the block being creted belongs to...
	
	@DELETE
	@Path("/{block}")
	void deleteBlock(@PathParam("block") String block);
	
	@GET
	@Path("/{block}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	byte[] readBlock(@PathParam("block") String block);
	
}

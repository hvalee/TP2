package sys.storage.rest.datanode;

import java.io.File;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import api.storage.Datanode;
import utils.IO;
import utils.Random;

public class DatanodeResources implements Datanode {

	private static final long MAX_BLOCKS_IN_CACHE = 128;

	protected final String baseURI;
	
	protected Cache<String, byte[]> blockCache = Caffeine.newBuilder()
			.maximumSize( MAX_BLOCKS_IN_CACHE )
			.build();

	public DatanodeResources(final String baseURI) {
		this.baseURI = baseURI + Datanode.PATH.substring(1) + "/";
	}

	@Override
	// Second parameter is only used in the GC version...
	public String createBlock(byte[] data, String blob) {
		String id = Random.key128();
		blockCache.put( id, data );
		IO.write( new File( id ), data);
		return baseURI.concat(id);
	}

	@Override
	public void deleteBlock(String block) {
		blockCache.invalidate( block );		
		File file = new File(block);
		if (file.exists())
			IO.delete(file);
		else
			throw new WebApplicationException(Status.NOT_FOUND);
	}

	@Override
	public byte[] readBlock(String block) {		
		byte[] data = blockCache.getIfPresent( block );
		if( data == null ) {
			File file = new File(block);
			if (file.exists()) {
				data = IO.read( file );
				blockCache.put( block, data);
			} else
				throw new WebApplicationException(Status.NOT_FOUND);
		}
		return data;			
	}		
}

package sys.storage.rest.datanode.gc;

import static utils.Log.Log;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import api.storage.Namenode;
import sys.storage.rest.RestNamenodeClient;
import sys.storage.rest.datanode.DatanodeResources;
import utils.Base58;
import utils.IO;
import utils.Random;

public class GcDatanodeResources extends DatanodeResources {

	private static final long EXPIRATION = 45;
	
	//Keeps the blobs that have blocks created but are not yet confirmed
	//to be in the namenode
	Cache<String, String> blobCache = Caffeine.newBuilder()
			.expireAfterWrite(EXPIRATION, TimeUnit.SECONDS)
			.removalListener( this::onRemoval )
			.build();

	//Keeps blobs that are know not to be in the namenode
	//When discarding some block's orphan siblings avoids contacting the namenode again...
	Cache<String, String> nonExistingBlobs = Caffeine.newBuilder()
			.expireAfterAccess(EXPIRATION, TimeUnit.SECONDS)
			.build();
	
	// The executor runs once a second so that the cache can purge expired entries...
	public GcDatanodeResources(final String baseURI) {
		super( baseURI );
		Executors.newScheduledThreadPool(2).scheduleAtFixedRate(() -> blobCache.cleanUp(), 0, 1, TimeUnit.SECONDS); 
	}

	@Override
	//The blob parameter is passed as an optional query parameter.
	public String createBlock(byte[] data, String blob) {
		String id = Random.key128();
		if( blob != null ) {
			String blobId = Base58.encode(blob.getBytes());
			id = blobId.concat("-").concat( id );
			blobCache.put( id, blob);			
		}
		
		blockCache.put( id, data );

		IO.write( new File( id ), data);
		return baseURI.concat(id);
	}

	@Override
	public void deleteBlock(String block) {
		blobCache.invalidate( block );
		super.deleteBlock(block);
	}

	@Override
	public byte[] readBlock(String block) {
		blobCache.invalidate( block );
		return super.readBlock(block);
	}
	

	public void onRemoval(String block, String blob, RemovalCause cause) {
		try {
			if( nonExistingBlobs.getIfPresent(blob) != null || namenode().read( blob ) == null) {
				nonExistingBlobs.put(blob, blob);
				Log.fine(String.format("deleting block: %s of blob: %s\n", block, blob));				
				this.deleteBlock( block );		
			}
		} catch( Exception x ) {
			x.printStackTrace();
		}
	}
	
	synchronized private Namenode namenode() {
		if( namenode == null ) 
			namenode = new RestNamenodeClient();
		return namenode;
	}
	
	private Namenode namenode;
}

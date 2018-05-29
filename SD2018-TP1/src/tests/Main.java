package tests;

import static java.util.stream.IntStream.range;

import java.util.List;
import java.util.stream.Collectors;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.rest.RestBlobStorage;
import utils.Random;

public class Main {

	private static final int MAX_BLOBS = 10;
	private static final int MAX_LINES = 200;

	public static void main(String[] args) {
		
		BlobStorage storage = new RestBlobStorage();
		
		List<String> blobs = range(0, MAX_BLOBS).boxed().map( i -> Random.key128() ).collect( Collectors.toList());
		
		blobs.forEach( blob -> {
			BlobWriter writer = storage.blobWriter( blob );
			range(0, MAX_LINES).forEach( i -> {
				writer.writeLine( Random.key128() );
			});
			writer.close();		
		});
		System.err.println("done...");
	}

}

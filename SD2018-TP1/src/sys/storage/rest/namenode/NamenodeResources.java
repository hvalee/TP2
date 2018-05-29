package sys.storage.rest.namenode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import api.storage.Namenode;

public class NamenodeResources implements Namenode {

	private Trie<String, List<String>> names = new PatriciaTrie<>();
	
	@Override
	synchronized public List<String> list(String prefix) {
		return new ArrayList<>(names.prefixMap( prefix ).keySet());
	}

	@Override
	synchronized public void create(String name,  List<String> metadata) {
		if( names.putIfAbsent(name, metadata) != null )
			throw new WebApplicationException( Status.CONFLICT );	
		System.err.println( name + "/" + metadata.size() );
	}

	@Override
	synchronized public void delete(String prefix) {
		Set<String> keys = names.prefixMap( prefix ).keySet();
		if( ! keys.isEmpty() )
			names.keySet().removeAll( new ArrayList<>(keys) );
		else
			throw new WebApplicationException( Status.NOT_FOUND );
	}

	@Override
	synchronized public void update(String name, List<String> metadata) {
		if( names.putIfAbsent( name, metadata) == null )
			throw new WebApplicationException( Status.NOT_FOUND );
	}

	@Override
	synchronized public List<String> read(String name) {
		List<String> metadata = names.get( name );
		if( metadata == null )
			throw new WebApplicationException( Status.NOT_FOUND );
		else
			return metadata;
	}
}

package sys.storage.dropbox.msgs;

public class CreateFileArgs {

	final String path;
	final String mode;
	final boolean autorename;
	final boolean mute;
	
	public CreateFileArgs(String path, String mode, boolean autorename, boolean mute) {
		this.path= path;
		this.mode= mode;
		this.autorename= autorename;
		this.mute= mute;
	}
}

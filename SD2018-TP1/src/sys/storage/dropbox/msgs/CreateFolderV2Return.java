package sys.storage.dropbox.msgs;

import java.util.HashMap;
import java.util.List;

public class CreateFolderV2Return {

    private List<CreateFolderV2Return.FolderEntry> entries;


    public static class FolderEntry extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        public FolderEntry() {
        }

        @Override
        public String toString() {

            return super.get("path_display").toString();
        }
    }

    public CreateFolderV2Return() {
    }

    public List<CreateFolderV2Return.FolderEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<CreateFolderV2Return.FolderEntry> entries) {
        this.entries = entries;
    }

}
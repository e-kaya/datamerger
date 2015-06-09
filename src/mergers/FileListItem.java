package mergers;

public class FileListItem {

	private String path;
	private String fileName;
	
	public FileListItem(String path, String fileName) {
		super();
		this.path = path;
		this.fileName = fileName;
	}

	public String getPath() {
		return path;
	}

	public String getFileName() {
		return fileName;
	}

}

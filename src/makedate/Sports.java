package makedate;



public class Sports {
    private int id,count,heartbeat;
    private String name,location,time;
    private float heat;
    
    public String toString(){
    	return this.id+" "+this.name+" "+this.time+" "+this.count+
    			" "+this.heartbeat+" "+this.heat+" "+this.location;
    }
    
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getHeartbeat() {
		return heartbeat;
	}
	public void setHeartbeat(int heartbeat) {
		this.heartbeat = heartbeat;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public float getHeat() {
		return heat;
	}
	public void setHeat(float heat) {
		this.heat = heat;
	}
    
    
}

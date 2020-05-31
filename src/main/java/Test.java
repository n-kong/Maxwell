import java.sql.*;

public class Test {


	private String driver ="com.mysql.jdbc.Driver";
	private String url="jdbc:mysql://218.108.16.34:6033/test";
	private String name="root";
	private String pwd="fenghuo123456";
	Connection conn=null;

	protected  Connection getconn(){
		conn=null;
		try {
			Class.forName(driver);
			conn= DriverManager.getConnection(url,name,pwd);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}


	public static void main(String[] args) {

		Connection getconn = new Test().getconn();

		try {
			PreparedStatement preparedStatement = getconn.prepareStatement("select * from fh_test");
			ResultSet resultSet = preparedStatement.executeQuery();

			System.out.println(resultSet.getFetchSize());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println(getconn.toString());

	}


	protected void closeAll(Connection conn , PreparedStatement ps, ResultSet rs){
		if(rs!=null)
			try {
				if(rs!=null)
					rs.close();
				if(ps!=null)
					ps.close();
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	public int executeUpdate(String sql ,Object []ob){
		conn=getconn();
		PreparedStatement ps=null;
		try {
			ps=prepareStatement(conn,sql,ob);
			int i=ps.executeUpdate();
			return i;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			return 0;
		}finally{
			closeAll(conn, ps, null);
		}

	}
	/***
	 * 查询方法
	 */
	protected PreparedStatement prepareStatement(Connection conn,String sql,Object []ob){
		PreparedStatement ps=null;
		try {
			int index=1;
			ps = conn.prepareStatement(sql);
			if(ps!=null&&ob!=null){
				for (int i = 0; i < ob.length; i++) {
					ps.setObject(index, ob[i]);
					index++;
				}
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return ps;
	}


}

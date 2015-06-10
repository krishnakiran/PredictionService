package com.infosys.spark.servlet;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * Servlet implementation class TestGLMServlet
 */
@WebServlet(name="TestModelTest",urlPatterns={"/TestGLM"})
public class TestModelServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServletFileUpload uploader = null;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public TestModelServlet() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    @Override
	public void init() throws ServletException {
		DiskFileItemFactory fileFactory = new DiskFileItemFactory();
		File filesDir = (File) getServletContext().getAttribute(
				"FILES_DIR_FILE");
		fileFactory.setRepository(filesDir);
		this.uploader = new ServletFileUpload(fileFactory);
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
	{
		try
		{
			List<FileItem> fileItemsList = uploader.parseRequest(request);
			Iterator<FileItem> fileItemsIterator = fileItemsList.iterator();
			while (fileItemsIterator.hasNext()) 
			{
				FileItem fileItem = fileItemsIterator.next();
				if (fileItem.isFormField()) 
				{
					continue;
				}
				else
				{
					System.out.println("FieldName=" + fileItem.getFieldName());
					System.out.println("FileName=" + fileItem.getName());
					System.out.println("ContentType=" + fileItem.getContentType());
					System.out.println("Size in bytes=" + fileItem.getSize());
					File file = new File(request.getServletContext().getAttribute(
							"FILES_DIR")
							+ File.separator + fileItem.getName());
					System.out.println("Absolute Path at server="
							+ file.getAbsolutePath());
					fileItem.write(file);
					String testFilePath = file.getAbsolutePath();
					request.getSession().setAttribute("testFilePath",testFilePath);
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	
	}

}

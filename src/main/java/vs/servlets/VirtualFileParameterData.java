package vs.servlets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.DataUnlimited.DataUnlimitedApi;
import vs.util.StringOps;

import java.io.IOException;
import java.util.stream.Collectors;

public class VirtualFileParameterData extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public VirtualFileParameterData() {
		super();
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String vfname = request.getParameter("dms-vfname");
		boolean result = new DataUnlimitedApi().removeVFFromGlobalVirtualFileParam(vfname);
		String responseString;
		if (result) {
			responseString = StringOps.append("<dmsv-status>DMSV-SUCCESS: Virtual file ", vfname,
					" successfully deleted</dmsv-status>");
			response.setStatus(200);
		} else {
			responseString = StringOps.append("<dmsv-status>DMSV-ERROR: Global Virtual file ", vfname,
					" not found</dmsv-status>");
			response.setStatus(400);
		}
		response.getWriter().append(responseString);
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String vfname = request.getParameter("dms-vfname");
		String responseString;
		if (vfname != null && vfname.length() > 0) {
			String result = new DataUnlimitedApi().getVFFromGlobalVirtualFileParam(vfname);
			if (result != null) {
				responseString = StringOps.append("<dms-g-vf>\n<dms-vfname>", vfname, "<dms-vfname>\n<dms-vfdata>",
						result, "<dms-vfdata>\n<dms-g-vf>");
				response.setStatus(200);
			} else {
				responseString = StringOps.append("<dmsv-status>DMSV-ERROR: Virtual file ", vfname,
						" not found</dmsv-status>");
				response.setStatus(400);
			}
		} else {
			responseString = new DataUnlimitedApi().getVFFromGlobalVirtualFileParam("");
			response.setStatus(200);
		}
		response.getWriter().append(responseString);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String ow = request.getHeader("dmsv-overwrite-existing");
		String result;
		if (ow != null && ow.equals("true"))
			result = new DataUnlimitedApi().createGlobalVirtualFileParam(
					request.getReader().lines().collect(Collectors.joining(System.lineSeparator())), true);
		else
			result = new DataUnlimitedApi().createGlobalVirtualFileParam(
					request.getReader().lines().collect(Collectors.joining(System.lineSeparator())), false);

		String responseString;
		if (!result.contains("Error")) {
			responseString = StringOps.append(
					"<dmsv-status>DMSV-SUCCESS: Virtual file(s) successfully created</dmsv-status>\n<dmsv-gvfp-log>\n",
					result, "\n<dmsv-gvfp-log>");
			response.setStatus(200);
		} else {
			responseString = StringOps.append(
					"<dmsv-status>DMSV-ERROR: Virtual file(s) might not not be created properly. Check input format.</dmsv-status>\n<dmsv-gvfp-log>\n",
					result, "\n<dmsv-gvfp-log>");
			response.setStatus(400);
		}
		response.getWriter().append(responseString);
	}

}

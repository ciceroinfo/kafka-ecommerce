package com.ciceroinfo;

import com.ciceroinfo.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    
    KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    
    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we are not caring about any security issues, we are only
            // showing how to use http as a starting point
            String orderId = req.getParameter("uuid");
            var amount = new BigDecimal(req.getParameter("amount"));
            var email = req.getParameter("email");
            var order = new Order(email, orderId, amount);
    
            try(var database = new OrdersDatabase()) {
                if (database.saveNew(order)) {
        
        
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
        
                    System.out.println("New order sent successfully");
                    resp.setStatus(HttpServletResponse.SC_CREATED);
                    resp.getWriter().println("New order sent successfully");
                } else {
                    System.out.println("Old order received");
                    resp.setStatus(HttpServletResponse.SC_CREATED);
                    resp.getWriter().println("Old order received");
                }
            }
        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException(e);
        }
    }
}

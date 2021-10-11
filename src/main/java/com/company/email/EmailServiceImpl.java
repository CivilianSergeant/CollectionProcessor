package com.company.email;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class EmailServiceImpl implements EmailService{

    private static final String primaryEmailAddress="mu.himel@gmail.com";
    private static final String secondaryAddress = "raisuddingc@gmail.com";
    private static final String cc = "momin@grameen.com";
//    private static final Logger logger = LoggerFactory.getLogger(EmailServiceImpl.class);

    @Override
    public void sendMail(String body) {

        try{

            Properties properties = new Properties();
            properties.setProperty("mail.smtp.host","smtp.gmail.com");
            properties.setProperty("mail.smtp.port","587");
            properties.setProperty("mail.smtp.auth","true");
            properties.setProperty("mail.smtp.starttls.enable","true");
            Session session = Session.getInstance(properties,new EmailAuthenticator());

            MimeMessage message = new MimeMessage(session);

            message.setSubject("Background Service Error");
            message.addRecipients(MimeMessage.RecipientType.TO,primaryEmailAddress);
            message.addRecipients(MimeMessage.RecipientType.CC,secondaryAddress);
            message.addRecipients(MimeMessage.RecipientType.CC,cc);
            message.addHeader("Content-Type","text/plain;charset=UTF-8");
            message.setText(body);
            Transport.send(message);

        }catch (MessagingException e){

//            logger.error(e.getLocalizedMessage());
        }

    }
}

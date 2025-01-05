package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private IncentiveService incentiveService;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void handleTransaction(Transaction transaction){
        System.out.println("Received Transaction:" + transaction);
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        System.out.println("Before transaction - Sender balance: " + sender.getBalance() + ", Recipient balance: " + recipient.getBalance());

        if (sender.getBalance() >= transaction.getAmount()) {
            // Process transaction: deduct from sender and add to recipient
            sender.setBalance(sender.getBalance() - transaction.getAmount());

            Incentive incentive = incentiveService.getIncentive(transaction);

            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            // Debugging point to check balances after transaction
            System.out.println("After transaction - Sender balance: " + sender.getBalance() + ", Recipient balance: " + recipient.getBalance());

            // Save the transaction record and update user balances
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive.getAmount());
            transactionRepository.save(record);
            userRepository.save(sender);
            userRepository.save(recipient);

//            if (sender.getName().equals("wilbur")) {
//                System.out.println("Wilbur's balance after transaction: " + recipient.getBalance());
//            }


        } else {
            // If balance is insufficient, discard transaction
            System.out.println("Transaction failed: Insufficient balance");
        }

    }
}

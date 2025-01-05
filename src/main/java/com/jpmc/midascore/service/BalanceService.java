package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class BalanceService {

    @Autowired
    private UserRepository userRepository;

    public Balance getBalance(Long userId){
        Optional<UserRecord> user = userRepository.findById(userId);

        if(!user.isPresent()){
            return new Balance(0.0f);
        }

        return new Balance(user.get().getBalance());
    }

}

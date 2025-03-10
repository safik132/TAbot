require("dotenv").config();
import axios from "axios";
import { LAMPORTS_PER_SOL } from "@solana/web3.js";
import { swapBuy, swapSell, getTokenBalance, getNetworkCongestion, getTokenPrice, getSolBalance } from "./swap";

// Define the structure of a token profile from DexScreener.
interface DexTokenProfile {
  url: string;
  chainId: string;
  tokenAddress: string;
  icon: string;
  header: string;
  description: string;
  links: Array<{ type: string; label: string; url: string }>;
}

// Constants for trade amounts and thresholds.
const SOL_AMOUNT = 0.001 * LAMPORTS_PER_SOL;
const NEW_PAIR_THRESHOLD_MS = 300000; // 5 minutes

// Concurrency limiter variables.
const MAX_CONCURRENT = 1;
let currentProcesses = 0;

// Sets to track tokens currently being processed and tokens already traded.
const processingTokens = new Set<string>();
const doneTokens = new Set<string>();

// Circuit breaker variables
let lastNetworkPauseTime = 0;
const NETWORK_PAUSE_COOLDOWN = 5 * 60 * 1000; // 5 minutes

// Track consecutive failures
let consecutiveFailures = 0;
const MAX_CONSECUTIVE_FAILURES = 3;

// Trading stats
const tradingStats = {
  totalTrades: 0,
  successfulTrades: 0,
  failedTrades: 0,
  totalProfit: 0
};

/**
 * Delay helper.
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Fetch latest token profiles.
 */
async function getLatestTokenProfiles(): Promise<DexTokenProfile[]> {
  try {
    // Try multiple times with backoff if needed
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await axios.get("https://api.dexscreener.com/token-profiles/latest/v1", {
          timeout: 10000, // 10 second timeout
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        });
        console.log(`[${new Date().toISOString()}] Token Profiles Response:`, response.data);
        return response.data;
      } catch (error) {
        if (attempt < 2) {
          console.warn(`[${new Date().toISOString()}] Retrying token profiles fetch (${attempt + 1}/3)`);
          await delay(2000 * Math.pow(2, attempt));
        } else {
          throw error;
        }
      }
    }
    return [];
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error fetching token profiles:`, error);
    return [];
  }
}

/**
 * Fetch dynamic trading data for a token.
 * The endpoint is:
 *   GET https://api.dexscreener.com/tokens/v1/{chainId}/{tokenAddress}
 * We assume the API returns an array; we use the first element.
 */
async function getTokenDynamicData(chainId: string, tokenAddress: string): Promise<any | null> {
  try {
    const url = `https://api.dexscreener.com/tokens/v1/${chainId}/${tokenAddress}`;
    const response = await axios.get(url, {
      timeout: 5000, // 5 second timeout
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    if (Array.isArray(response.data) && response.data.length > 0) {
      return response.data[0];
    }
    return null;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error fetching dynamic data for ${tokenAddress}:`, error);
    return null;
  }
}

/**
 * Helper: Sums up buy transactions from a pair's transaction data.
 */
function getTotalBuys(txns: any): number {
  let totalBuys = 0;
  if (txns && typeof txns === "object") {
    for (const key in txns) {
      if (txns.hasOwnProperty(key) && typeof txns[key].buys === "number") {
        totalBuys += txns[key].buys;
      }
    }
  }
  return totalBuys;
}

/**
 * Check if network is too congested for trading
 */
async function shouldPauseTrading(): Promise<{ pause: boolean; congestionFactor: number }> {
  try {
    const congestion = await getNetworkCongestion();
    
    // Always return false for pause, but provide the congestion factor
    // for fee adjustment purposes
    console.log(`[${new Date().toISOString()}] Network congestion factor: ${congestion.toFixed(2)} - continuing to trade`);
    return { pause: false, congestionFactor: congestion };
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error checking network status:`, error);
    return { pause: false, congestionFactor: 1.0 };
  }
}

/**
 * Process token using its dynamic data.
 * This function checks if the token meets criteria, buys it,
 * monitors for profit, and sells when conditions are met.
 */
async function processTokenData(
  tokenProfile: DexTokenProfile, 
  dynamicData: any,
  congestionFactor: number = 1.0
): Promise<boolean> {
  try {
    const tokenAddress = tokenProfile.tokenAddress;
    const tokenName = tokenProfile.header || "Unknown";
    console.log(`[${new Date().toISOString()}] Processing token ${tokenAddress} (${tokenName}) with dynamic data...`);

    const now = Date.now();
    const pairAge = dynamicData.pairCreatedAt ? now - dynamicData.pairCreatedAt : Number.MAX_VALUE;
    const liquidityUSD = (dynamicData.liquidity && typeof dynamicData.liquidity.usd === "number") ? dynamicData.liquidity.usd : 0;
    const volumeUSD = (dynamicData.volume && typeof dynamicData.volume.h24 === "number") ? dynamicData.volume.h24 : 0;
    const totalBuys = getTotalBuys(dynamicData.txns);
    console.log(
      `[${new Date().toISOString()}] Token dynamic data for ${tokenName}: Age: ${pairAge}ms, Liquidity: $${liquidityUSD}, Volume: $${volumeUSD}, Buys: ${totalBuys}, Dex: ${dynamicData.dexId}`
    );

    // Ensure the token is trading on Raydium.
    if (!(dynamicData.dexId && dynamicData.dexId.toLowerCase() === "raydium")) {
      console.log(`[${new Date().toISOString()}] Token ${tokenAddress} (${tokenName}) is not on Raydium. Skipping.`);
      return false;
    }

    // Check filtering criteria.
    if (pairAge > NEW_PAIR_THRESHOLD_MS || liquidityUSD <= 10000 || volumeUSD <= 50000 || totalBuys <= 100) {
      console.log(`[${new Date().toISOString()}] Token ${tokenAddress} (${tokenName}) does not meet criteria. Skipping.`);
      return false;
    }

    // If congestion is truly extreme, log but don't abort
    if (congestionFactor > 6) {
      console.log(`[${new Date().toISOString()}] WARNING: Extreme network congestion (${congestionFactor.toFixed(2)}) detected. Trading will be very difficult.`);
    }

    // More dynamic parameters based on actual network conditions
    // Start with an aggressive baseline fee multiplier that scales with congestion
    let initialFeeMultiplier = congestionFactor * 1.5;
    
    // Adjust slippage based on congestion
    let customSlippage = 6; // Higher default slippage
    let maxBuyAttempts = 12;
    
    // For high congestion, use even more aggressive parameters
    if (congestionFactor >= 3.0) {
      initialFeeMultiplier = congestionFactor * 2.0;
      customSlippage = 8; // 8% slippage tolerance
      maxBuyAttempts = 15;
    }
    
    // For extreme congestion, go all out
    if (congestionFactor >= 5.0) {
      initialFeeMultiplier = congestionFactor * 3.0;
      customSlippage = 10; // 10% slippage tolerance
      maxBuyAttempts = 18;
      console.log(`[${new Date().toISOString()}] Extreme congestion detected. Using maximum aggressive parameters: fee multiplier ${initialFeeMultiplier.toFixed(2)}, slippage ${customSlippage}%, max attempts ${maxBuyAttempts}`);
    }
    
    // Store pre-trade SOL balance to calculate actual profit later
    const preTradeSolBalance = await getSolBalance();
    
    // BUY: Use our enhanced parameters
    let buyResult = { success: false, fee: initialFeeMultiplier };
    const buyDeadline = Date.now() + 120000; // 2 minutes total time allowed for buy
    let buyAttempt = 1;
    
    // If congestion is high, save time by starting with more aggressive parameters
    if (congestionFactor >= 4.0) {
      console.log(`[${new Date().toISOString()}] High congestion detected. Starting with aggressive buy parameters.`);
      buyResult.fee = initialFeeMultiplier * 1.5;
    }
    
    while (Date.now() < buyDeadline && buyAttempt <= maxBuyAttempts) {
      console.log(`[${new Date().toISOString()}] Attempting BUY (${buyAttempt}/${maxBuyAttempts}) for token ${tokenAddress} (${tokenName}) with fee multiplier ${buyResult.fee.toFixed(2)}...`);
      
      // If we're getting close to the deadline, increase slippage and fees further
      const remainingTime = buyDeadline - Date.now();
      let attempSlippage = customSlippage;
      if (remainingTime < 30000) { // Less than 30 seconds left
        buyResult.fee = Math.min(buyResult.fee * 1.5, 20);
        attempSlippage = customSlippage + 3;
        console.log(`[${new Date().toISOString()}] Time running out, increasing buy urgency: fee ${buyResult.fee.toFixed(2)}, slippage ${attempSlippage}%`);
      }
      
      // Pass customSlippage to the swapBuy function
      buyResult = await swapBuy(tokenAddress, SOL_AMOUNT, buyResult.fee, 0, attempSlippage);
      
      if (buyResult.success) {
        console.log(`[${new Date().toISOString()}] BUY successful for token ${tokenAddress} (${tokenName}).`);
        break;
      } else {
        // More aggressive backoff for high congestion
        const backoffDelay = congestionFactor >= 4.0 
          ? Math.min(2000 * Math.pow(1.3, buyAttempt - 1), 10000)
          : Math.min(5000 * Math.pow(1.5, buyAttempt - 1), 15000);
        
        console.warn(`[${new Date().toISOString()}] BUY attempt ${buyAttempt} failed for ${tokenAddress} (${tokenName}). Retrying in ${backoffDelay/1000}s...`);
        await delay(backoffDelay);
        
        // More aggressive fee increase for subsequent attempts
        buyResult.fee = Math.min(buyResult.fee * 1.5, 20);
        buyAttempt++;
      }
    }
    
    if (!buyResult.success) {
      console.error(`[${new Date().toISOString()}] Failed to BUY ${tokenAddress} (${tokenName}) after ${buyAttempt-1} attempts. Aborting.`);
      return false;
    }

    // Record buy price after successful purchase
    let buyPrice = await getTokenPrice(tokenAddress);
    console.log(`[${new Date().toISOString()}] Buy price for ${tokenName}: ${buyPrice || 'unknown'} SOL per token`);

    // Reduced wait time from 60 seconds to 10-15 seconds
    console.log(`[${new Date().toISOString()}] Waiting 10 seconds before checking profit for ${tokenAddress} (${tokenName})...`);
    await delay(10000);

    // Start profit monitoring
    const sellDeadline = Date.now() + 90 * 1000; // 90 second max hold time (reduced from 2 minutes)
    const MIN_PROFIT_PERCENTAGE = 5; // Sell if at least 5% profit
    const MAX_LOSS_PERCENTAGE = -10; // Stop loss at 10% loss
    let shouldSell = false;
    let sellReason = "max hold time reached";
    
    // More frequent profit checking
    while (Date.now() < sellDeadline && !shouldSell) {
      const tokenBalance = await getTokenBalance(tokenAddress);
      if (tokenBalance <= 0) {
        console.error(`[${new Date().toISOString()}] No token balance for ${tokenAddress} (${tokenName}). Aborting SELL.`);
        return false;
      }
      
      const currentPrice = await getTokenPrice(tokenAddress);
      if (!currentPrice || !buyPrice) {
        console.warn(`[${new Date().toISOString()}] Could not determine price for ${tokenName}. Will check again.`);
        await delay(3000);
        continue;
      }
      
      const profitPercentage = ((currentPrice - buyPrice) / buyPrice) * 100;
      console.log(`[${new Date().toISOString()}] Current profit for ${tokenName}: ${profitPercentage.toFixed(2)}% (${currentPrice} SOL per token)`);
      
      // Sell conditions
      if (profitPercentage >= MIN_PROFIT_PERCENTAGE) {
        console.log(`[${new Date().toISOString()}] Profit target reached (${profitPercentage.toFixed(2)}%). Selling...`);
        shouldSell = true;
        sellReason = `profit target ${MIN_PROFIT_PERCENTAGE}% reached`;
        break;
      }
      
      if (profitPercentage <= MAX_LOSS_PERCENTAGE) {
        console.log(`[${new Date().toISOString()}] Stop loss triggered (${profitPercentage.toFixed(2)}%). Selling...`);
        shouldSell = true;
        sellReason = `stop loss ${MAX_LOSS_PERCENTAGE}% triggered`;
        break;
      }
      
      // Dynamic profit taking in congested network conditions
      // If very high congestion, take profits earlier
      if (congestionFactor >= 4.0 && profitPercentage >= MIN_PROFIT_PERCENTAGE * 0.6) {
        console.log(`[${new Date().toISOString()}] Taking smaller profit (${profitPercentage.toFixed(2)}%) due to high network congestion...`);
        shouldSell = true;
        sellReason = `taking smaller profit in high congestion`;
        break;
      }
      
      // Also consider selling if we're approaching deadline and have any profit
      const timeRemaining = sellDeadline - Date.now();
      if (timeRemaining < 20000 && profitPercentage > 0) { // Less than 20 seconds left
        console.log(`[${new Date().toISOString()}] Taking profit (${profitPercentage.toFixed(2)}%) as hold deadline is approaching...`);
        shouldSell = true;
        sellReason = `taking profit before deadline`;
        break;
      }
      
      // Check again more frequently
      await delay(3000); // Check profit every 3 seconds
    }
    
    console.log(`[${new Date().toISOString()}] Preparing to sell ${tokenName} (reason: ${sellReason})`);

    const tokenBalance = await getTokenBalance(tokenAddress);
    if (tokenBalance <= 0) {
      console.error(`[${new Date().toISOString()}] No token balance for ${tokenAddress} (${tokenName}). Aborting SELL.`);
      return false;
    }

    // For the sell part, use even more aggressive parameters based on congestion
    // Start with a higher fee multiplier for selling
    let sellFeeMultiplier = buyResult.fee * 1.8;
    let sellSlippage = customSlippage + 3; // Higher slippage for sell
    
    // For high congestion, use extremely aggressive sell parameters
    if (congestionFactor >= 4.0) {
      sellFeeMultiplier = buyResult.fee * 2.5;
      sellSlippage = customSlippage + 5;
      console.log(`[${new Date().toISOString()}] Using extreme sell parameters due to high congestion: fee ${sellFeeMultiplier.toFixed(2)}, slippage ${sellSlippage}%`);
    }
    
    let sellResult = { success: false, fee: sellFeeMultiplier };
const finalSellDeadline = Date.now() + 3 * 60 * 1000; // 3 minutes
let sellAttempt = 1;
const maxSellAttempts = congestionFactor >= 3.0 ? 25 : 20;
const initialTokenBalance = await getTokenBalance(tokenAddress);

while (Date.now() < finalSellDeadline && sellAttempt <= maxSellAttempts) {
  const currentBalance = await getTokenBalance(tokenAddress);
  
  // If balance is significantly reduced, transaction probably worked despite confirmation errors
  if (currentBalance <= 0 || currentBalance < initialTokenBalance * 0.1) {
    console.log(`[${new Date().toISOString()}] Token balance reduced to ${currentBalance}. Considering sell successful.`);
    sellResult.success = true;
    break;
  }
  
  console.log(
    `[${new Date().toISOString()}] Attempting SELL (${sellAttempt}/${maxSellAttempts}) for token ${tokenAddress} (${tokenName}) (balance: ${currentBalance}) with fee multiplier ${sellResult.fee.toFixed(2)}...`
  );
      
      // If we're getting close to the deadline, increase urgency even more
      const remainingTime = finalSellDeadline - Date.now();
      let attemptSlippage = sellSlippage;
      if (remainingTime < 60000) { // Less than 1 minute left
        sellResult.fee = Math.min(sellResult.fee * 1.5, 25);
        attemptSlippage = sellSlippage + 5;
        console.log(`[${new Date().toISOString()}] Sell time running out, increasing urgency: fee ${sellResult.fee.toFixed(2)}, slippage ${attemptSlippage}%`);
      }
      
      // Pass customSlippage to the swapSell function
      sellResult = await swapSell(tokenAddress, currentBalance, sellResult.fee, 0, attemptSlippage);
      
      if (sellResult.success) {
        console.log(`[${new Date().toISOString()}] SELL successful for token ${tokenAddress} (${tokenName}).`);
        break;
      } else {
        const backoffDelay = Math.min(3000 * Math.pow(1.3, sellAttempt - 1), 15000);
        console.warn(`[${new Date().toISOString()}] SELL attempt ${sellAttempt} failed for ${tokenAddress} (${tokenName}). Retrying in ${backoffDelay/1000}s...`);
        await delay(backoffDelay);
        
        // More aggressive fee increase for subsequent attempts
        sellResult.fee = Math.min(sellResult.fee * 1.4, 30);
        sellAttempt++;
      }
    }
    
    // If full sell failed, try selling in smaller chunks
    if (!sellResult.success) {
      console.error(`[${new Date().toISOString()}] Failed to SELL full token amount. Checking actual balance before trying chunks...`);
      
      // Check current balance - the sell might have actually succeeded
      const remainingBalance = await getTokenBalance(tokenAddress);
      
      // If balance is very low or zero, consider it a success despite the error
      if (remainingBalance <= 0 || remainingBalance < initialTokenBalance * 0.1) {
        console.log(`[${new Date().toISOString()}] Token balance is already very low (${remainingBalance}). Considering sell successful.`);
        return true;
      }
      
      console.log(`[${new Date().toISOString()}] Significant balance remains (${remainingBalance}). Attempting to sell in chunks...`);
      
      // Try selling in smaller chunks
      const chunkSizes = [0.5, 0.3, 0.2]; // 50%, 30%, 20% chunks
      let successCount = 0;
      
      for (const chunkSize of chunkSizes) {
        // Check balance again before each chunk attempt
        const currentBalance = await getTokenBalance(tokenAddress);
        if (currentBalance <= 0 || currentBalance < initialTokenBalance * 0.1) {
          console.log(`[${new Date().toISOString()}] Balance reduced to ${currentBalance}. No need for further chunks.`);
          return true;
        }
        
        const chunkAmount = currentBalance * chunkSize;
        if (chunkAmount <= 0) continue;
        
        console.log(`[${new Date().toISOString()}] Attempting to sell ${chunkAmount} tokens (${chunkSize * 100}% chunk of remaining ${currentBalance})`);
        
        // Use even higher fees and slippage for chunks
        const chunkResult = await swapSell(
          tokenAddress, 
          chunkAmount, 
          sellResult.fee * 1.5, 
          0, 
          sellSlippage + 10
        );
        
        if (chunkResult.success) {
          successCount++;
          console.log(`[${new Date().toISOString()}] Successfully sold chunk ${successCount}`);
          await delay(5000); // Short delay between chunks
        } else {
          // Even if the chunk "failed", check if balance decreased
          const postChunkBalance = await getTokenBalance(tokenAddress);
          if (postChunkBalance < currentBalance * 0.8) {
            console.log(`[${new Date().toISOString()}] Chunk appears successful despite error. Balance reduced from ${currentBalance} to ${postChunkBalance}`);
            successCount++;
          } else {
            console.error(`[${new Date().toISOString()}] Failed to sell chunk`);
          }
        }
      }
      
      // Final balance check to determine overall success
      const finalBalance = await getTokenBalance(tokenAddress);
      const significantReduction = finalBalance < initialTokenBalance * 0.3; // 70%+ sold
      if (significantReduction) {
        console.log(`[${new Date().toISOString()}] Successfully sold most tokens. Remaining: ${finalBalance}`);
        return true;
      }
    }

    // Calculate actual profit/loss
    const postTradeSolBalance = await getSolBalance();
    const profitInSol = postTradeSolBalance - preTradeSolBalance;
    console.log(`[${new Date().toISOString()}] Trade completed for ${tokenName}. Profit/loss: ${profitInSol.toFixed(6)} SOL`);

    // Update trading stats
    tradingStats.totalTrades++;
    tradingStats.totalProfit += profitInSol;
    
    // Final check for remaining balance
    const finalBalance = await getTokenBalance(tokenAddress);
    const success = finalBalance < tokenBalance * 0.1; // Consider success if sold 90%+
    
    if (success) {
      tradingStats.successfulTrades++;
      consecutiveFailures = 0; // Reset consecutive failures
    } else {
      tradingStats.failedTrades++;
      consecutiveFailures++;
      console.warn(`[${new Date().toISOString()}] Could not sell entire position. Remaining balance: ${finalBalance} tokens.`);
    }

    return success;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error processing token ${tokenProfile.tokenAddress}:`, error);
    consecutiveFailures++;
    return false;
  }
}

/**
 * Concurrency-limited wrapper for processing a token.
 */
async function processTokenWithLimit(tokenProfile: DexTokenProfile, dynamicData: any, congestionFactor: number = 1.0): Promise<boolean> {
  while (currentProcesses >= MAX_CONCURRENT) {
    await delay(1000);
  }
  currentProcesses++;
  try {
    const result = await processTokenData(tokenProfile, dynamicData, congestionFactor);
    return result;
  } finally {
    currentProcesses--;
  }
}

/**
 * Main loop:
 * 1. Fetch the latest token profiles.
 * 2. For each token profile on the Solana chain, fetch dynamic trading data.
 * 3. Log detailed info (name, dex, volume, liquidity, buys, age, eligibility).
 * 4. If eligible based on criteria, process the token (buy then sell).
 */
async function main() {
  console.log(`[${new Date().toISOString()}] Starting DexScreener trading bot using token profiles and dynamic data...`);
  while (true) {
    try {
      // Check network congestion but don't pause, just adjust parameters
      const { pause, congestionFactor } = await shouldPauseTrading();
      console.log(`[${new Date().toISOString()}] Network congestion factor: ${congestionFactor.toFixed(2)} - adjusting trade parameters`);
      
      // If we've had multiple consecutive failures, pause briefly to let network conditions improve
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        console.log(`[${new Date().toISOString()}] ${consecutiveFailures} consecutive failures detected. Pausing for 30 seconds to let network conditions improve...`);
        await delay(30000);
        consecutiveFailures = 0;
      }
      
      console.log(`[${new Date().toISOString()}] Fetching latest token profiles...`);
      const profiles = await getLatestTokenProfiles();
      const currentTime = Date.now();
      console.log(`[${new Date().toISOString()}] Found ${profiles.length} token profiles.`);

      // Log current trading stats every polling cycle
      console.log(`[${new Date().toISOString()}] Trading stats - Total: ${tradingStats.totalTrades}, Success: ${tradingStats.successfulTrades}, Failed: ${tradingStats.failedTrades}, Profit: ${tradingStats.totalProfit.toFixed(6)} SOL`);

      for (const profile of profiles) {
        // For now, we only process tokens on the Solana chain.
        if (profile.chainId.toLowerCase() !== "solana") continue;
        const tokenAddress = profile.tokenAddress;
        if (!tokenAddress) continue;
        if (processingTokens.has(tokenAddress) || doneTokens.has(tokenAddress)) continue;

        console.log(`[${new Date().toISOString()}] Fetching dynamic data for token ${tokenAddress} (${profile.header})...`);
        const dynamicData = await getTokenDynamicData(profile.chainId, tokenAddress);
        if (!dynamicData) {
          console.log(`[${new Date().toISOString()}] No dynamic data for token ${tokenAddress}. Skipping.`);
          continue;
        }
        const pairAge = dynamicData.pairCreatedAt ? currentTime - dynamicData.pairCreatedAt : Number.MAX_VALUE;
        const liquidityUSD = (dynamicData.liquidity && typeof dynamicData.liquidity.usd === "number") ? dynamicData.liquidity.usd : 0;
        const volumeUSD = (dynamicData.volume && typeof dynamicData.volume.h24 === "number") ? dynamicData.volume.h24 : 0;
        const totalBuys = getTotalBuys(dynamicData.txns);
        const eligible = (pairAge <= NEW_PAIR_THRESHOLD_MS &&
                          liquidityUSD > 10000 &&
                          volumeUSD > 50000 &&
                          totalBuys > 100 &&
                          dynamicData.dexId &&
                          dynamicData.dexId.toLowerCase() === "raydium");
        console.log(
          `[${new Date().toISOString()}] Token: ${profile.header} | Dex: ${dynamicData.dexId} | Volume: $${volumeUSD} | Liquidity: $${liquidityUSD} | Buys: ${totalBuys} | Age: ${pairAge}ms | Eligible: ${eligible}`
        );
        if (!eligible) {
          console.log(`[${new Date().toISOString()}] Skipping token ${profile.header} (${tokenAddress}) as not eligible.`);
          continue;
        }

        processingTokens.add(tokenAddress);
        processTokenWithLimit(profile, dynamicData, congestionFactor)
          .then((result) => {
            if (result) {
              doneTokens.add(tokenAddress);
              console.log(`[${new Date().toISOString()}] Successfully processed token ${profile.header} (${tokenAddress})`);
            } else {
              console.warn(`[${new Date().toISOString()}] Failed to fully process token ${profile.header} (${tokenAddress})`);
            }
            processingTokens.delete(tokenAddress);
          })
          .catch((error) => {
            console.error(`[${new Date().toISOString()}] Error processing token ${tokenAddress}:`, error);
            processingTokens.delete(tokenAddress);
            consecutiveFailures++;
          });
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error in main loop:`, error);
      consecutiveFailures++;
      
      // If we get errors in the main loop, pause briefly before continuing
      if (consecutiveFailures >= 2) {
        console.log(`[${new Date().toISOString()}] Multiple main loop errors. Pausing for recovery...`);
        await delay(15000);
      }
    }
    console.log(`[${new Date().toISOString()}] Polling cycle complete. Waiting 30 seconds before next cycle...`);
    await delay(30000);
  }
}

main();
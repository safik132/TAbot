import bs58 from "bs58";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
  VersionedTransaction,
  Commitment
} from "@solana/web3.js";
import { NATIVE_MINT, getAssociatedTokenAddress } from "@solana/spl-token";
import axios from "axios";
import { API_URLS } from "@raydium-io/raydium-sdk-v2";

const isV0Tx = true;

// Define multiple RPC endpoints for fallback
const RPC_ENDPOINTS = [
  process.env.RPC_URL!,
  process.env.FALLBACK_RPC_URL1,
  process.env.FALLBACK_RPC_URL2
].filter(Boolean); // Filter out undefined endpoints

// Initialize with primary RPC
let currentRpcIndex = 0;
const currentEndpoint = RPC_ENDPOINTS[currentRpcIndex] || process.env.RPC_URL!;

// Derive WS_URL from environment or from RPC_URL if not explicitly set.
const WS_URL =
  process.env.WS_URL ||
  currentEndpoint?.replace("https://", "wss://").replace(/\/$/, "");

let connection = new Connection(currentEndpoint, { wsEndpoint: WS_URL });
const owner = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY!));
const slippage = 5; // 5% default slippage tolerance

// Function to switch RPC endpoint on failure
function switchRpcEndpoint() {
  if (RPC_ENDPOINTS.length <= 1) return false;
  
  currentRpcIndex = (currentRpcIndex + 1) % RPC_ENDPOINTS.length;
  const newEndpoint = RPC_ENDPOINTS[currentRpcIndex];
  if (newEndpoint) {
    console.log(`[${new Date().toISOString()}] Switching to RPC endpoint: ${currentRpcIndex+1}`);
    
    // Create new connection with new endpoint
    const wsEndpoint = newEndpoint.replace("https://", "wss://").replace(/\/$/, "");
    connection = new Connection(newEndpoint, { wsEndpoint });
    
    // Re-establish WebSocket subscription
    setupBlockhashSubscription();
    return true;
  }
  return false;
}

// Global cache for the latest blockhash data updated via WebSocket subscription.
let cachedBlockhashData: { blockhash: string; lastValidBlockHeight: number; timestamp: number } | null = null;

// Cache for network congestion data
let congestionCache: { factor: number; timestamp: number } | null = null;
const CONGESTION_CACHE_TTL = 10000; // 10 seconds

// Subscribe to slot changes for up-to-date blockhash
function setupBlockhashSubscription() {
  connection.onSlotChange(async (slotInfo) => {
    try {
      const blockhashData = await connection.getLatestBlockhash({ commitment: "finalized" });
      cachedBlockhashData = { 
        ...blockhashData, 
        timestamp: Date.now() 
      };
      console.log(`[${new Date().toISOString()}] Updated cached blockhash: ${blockhashData.blockhash}`);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error fetching latest blockhash:`, error);
    }
  });
}

// Initial subscription setup
setupBlockhashSubscription();

/**
 * Helper: delay for the given milliseconds.
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Get the latest blockhash, preferring cached value if available and recent.
 */
async function getLatestBlockhash() {
  // If cached blockhash is older than 5 seconds, fetch a new one
  if (!cachedBlockhashData || Date.now() - cachedBlockhashData.timestamp > 5000) {
    try {
      const newBlockhashData = await connection.getLatestBlockhash({ commitment: "finalized" });
      cachedBlockhashData = { 
        ...newBlockhashData, 
        timestamp: Date.now() 
      };
      return newBlockhashData;
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error getting blockhash, retrying:`, error);
      
      // Try switching RPC endpoint if this fails
      if (switchRpcEndpoint()) {
        await delay(1000);
        return connection.getLatestBlockhash({ commitment: "finalized" });
      }
      
      // If we can't get a blockhash, this is a serious issue
      throw new Error("Failed to get blockhash after retry");
    }
  }
  return {
    blockhash: cachedBlockhashData.blockhash,
    lastValidBlockHeight: cachedBlockhashData.lastValidBlockHeight
  };
}

/**
 * Estimate network congestion based on transaction volume and performance metrics.
 * Returns a multiplier for transaction fees.
 */
export async function getNetworkCongestion(): Promise<number> {
  try {
    // Use cached value if available and recent
    if (congestionCache && Date.now() - congestionCache.timestamp < CONGESTION_CACHE_TTL) {
      return congestionCache.factor;
    }
    
    // Get recent performance samples
    const perfSamples = await connection.getRecentPerformanceSamples(5);
    if (perfSamples.length === 0) {
      congestionCache = { factor: 1.5, timestamp: Date.now() }; // Default to slightly higher
      return 1.5;
    }
    
    // Calculate average transactions per slot and slot time
    const avgTxPerSlot = perfSamples.reduce((sum, sample) => sum + sample.numTransactions, 0) / perfSamples.length;
    const avgSlotTime = perfSamples.reduce((sum, sample) => sum + sample.samplePeriodSecs, 0) / perfSamples.length;
    
    // More aggressive congestion detection
    let factor = 1.0;
    if (avgTxPerSlot > 4500) factor = 6.0;
    else if (avgTxPerSlot > 3500) factor = 4.5;
    else if (avgTxPerSlot > 2500) factor = 3.0;
    else if (avgTxPerSlot > 1500) factor = 2.0;
    else if (avgTxPerSlot > 800) factor = 1.5;
    
    // Adjust for slow slot times which indicates congestion
    if (avgSlotTime > 0.6) factor *= 1.5;
    
    // Cache the result
    congestionCache = { factor, timestamp: Date.now() };
    console.log(`[${new Date().toISOString()}] Network congestion factor: ${factor.toFixed(2)} (txs/slot: ${avgTxPerSlot.toFixed(0)}, slot time: ${avgSlotTime.toFixed(2)}s)`);
    return factor;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error getting network congestion:`, error);
    return 2.0; // Higher default multiplier to be safe
  }
}

/**
 * Exponential backoff wrapper for axios requests.
 */
async function axiosRequestWithBackoff(config: any, maxRetries = 5): Promise<any> {
  let retries = 0;
  while (true) {
    try {
      return await axios.request(config);
    } catch (error: any) {
      if ((error.response && error.response.status === 429 || 
           error.code === 'ECONNRESET' || 
           error.code === 'ETIMEDOUT' || 
           error.message.includes('timeout')) && 
          retries < maxRetries) {
        
        const retryAfter =
          parseInt(error.response?.headers?.["retry-after"] || "0", 10) * 1000;
        const delayTime = retryAfter || Math.pow(2, retries) * 1000;
        console.warn(`[${new Date().toISOString()}] API request failed. Retrying in ${delayTime}ms...`);
        await delay(delayTime);
        retries++;
        continue;
      } else {
        throw error;
      }
    }
  }
}

/**
 * Helper function to confirm transaction with retries
 */
async function confirmTransactionWithRetry(
  signature: string, 
  blockhash: string, 
  lastValidBlockHeight: number,
  maxRetries = 3
): Promise<boolean> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      await connection.confirmTransaction(
        {
          blockhash,
          lastValidBlockHeight,
          signature,
        },
        "confirmed"
      );
      return true;
    } catch (error) {
      // Check if transaction was actually confirmed despite the error
      const status = await connection.getSignatureStatus(signature);
      if (status?.value?.confirmationStatus === 'confirmed' || 
          status?.value?.confirmationStatus === 'finalized') {
        return true;
      }
      
      if (attempt < maxRetries - 1) {
        console.warn(`[${new Date().toISOString()}] Confirmation retry ${attempt+1}/${maxRetries} for tx ${signature}`);
        await delay(2000);
      }
    }
  }
  
  // One final check for confirmation before returning false
  const finalStatus = await connection.getSignatureStatus(signature);
  return finalStatus?.value?.confirmationStatus === 'confirmed' || 
         finalStatus?.value?.confirmationStatus === 'finalized';
}

/**
 * swapBuy: Swaps SOL for the given token using Raydium.
 * Returns an object { success, fee }.
 */
export async function swapBuy(
  tokenAddress: string,
  amount: number,
  priorityFeeMultiplier: number = 1,
  retryCount: number = 0,
  slippageOverride: number | null = null
): Promise<{ success: boolean; fee: number }> {
  const MAX_RETRIES = 12;
  const MAX_FEE_MULTIPLIER = 15;
  
  try {
    // Get fresh blockhash
    const { blockhash, lastValidBlockHeight } = await getLatestBlockhash();
    
    // Get priority fee data with retry
    let baseFee = 1000000; // Default high value in case API fails
    try {
      const feeUrl = `${API_URLS.BASE_HOST}${API_URLS.PRIORITY_FEE}`;
      const feeResponse = await axiosRequestWithBackoff({ method: "get", url: feeUrl });
      const feeData = feeResponse.data;
      baseFee = Number(feeData.data.default.h);
    } catch (error) {
      console.warn(`[${new Date().toISOString()}] Fee API error, using default high fee`);
    }
    
    // Increase fee multiplier based on retry count
    const dynamicMultiplier = priorityFeeMultiplier * (1 + (retryCount * 0.3));
    const computeUnitPriceMicroLamports = String(Math.floor(baseFee * dynamicMultiplier));
    
    // Increase slippage on retries
    const effectiveSlippage = slippageOverride !== null ? 
      slippageOverride : 
      (retryCount > 3 ? slippage + (retryCount - 3) : slippage);
    
    console.log(`[${new Date().toISOString()}] swapBuy using slippage: ${effectiveSlippage}%, fee multiplier: ${dynamicMultiplier.toFixed(2)}`);

    // Get swap quote
    const swapQuoteUrl = `${API_URLS.SWAP_HOST}/compute/swap-base-in?inputMint=${NATIVE_MINT}&outputMint=${tokenAddress}&amount=${amount}&slippageBps=${effectiveSlippage * 100}&txVersion=V0`;
    const swapQuoteResponse = await axiosRequestWithBackoff({ method: "get", url: swapQuoteUrl });
    const swapResponse = swapQuoteResponse.data;

    // Request transaction with higher compute limit
    const swapTxUrl = `${API_URLS.SWAP_HOST}/transaction/swap-base-in`;
    const swapTxResponse = await axiosRequestWithBackoff({
      method: "post",
      url: swapTxUrl,
      data: {
        computeUnitPriceMicroLamports,
        swapResponse,
        txVersion: "V0",
        wallet: owner.publicKey.toBase58(),
        wrapSol: true,
        unwrapSol: false,
        computeUnitLimit: 200000, // Explicit higher compute limit
      },
    });
    const swapTransactions = swapTxResponse.data;

    console.log(`[${new Date().toISOString()}] swapBuy parameters:`, {
      computeUnitPriceMicroLamports,
      wallet: owner.publicKey.toBase58(),
      wrapSol: true,
      unwrapSol: false,
      multiplier: dynamicMultiplier,
    });
    console.log(`[${new Date().toISOString()}] swapBuy transactions received:`, swapTransactions);

    // Process transactions
    const allTxBuf = swapTransactions.data.map((tx: { transaction: string }) =>
      Buffer.from(tx.transaction, "base64")
    );
    const allTransactions = allTxBuf.map((txBuf: Buffer) =>
      isV0Tx ? VersionedTransaction.deserialize(txBuf) : Transaction.from(txBuf)
    );

    for (const [idx, tx] of allTransactions.entries()) {
      tx.sign([owner]);
      
      // More aggressive transaction options
      const txOptions = {
        skipPreflight: true,
        maxRetries: 7,
        preflightCommitment: "processed" as Commitment
      };
      
      // Send with retries
      let txId = "";
      let sendSuccess = false;
      
      for (let sendAttempt = 0; sendAttempt < 3; sendAttempt++) {
        try {
          txId = await connection.sendTransaction(tx as VersionedTransaction, txOptions);
          sendSuccess = true;
          break;
        } catch (sendError: any) {
          if (sendAttempt < 2) {
            console.warn(`[${new Date().toISOString()}] Send retry ${sendAttempt+1}/3: ${sendError.message}`);
            await delay(1000);
          } else {
            throw sendError;
          }
        }
      }
      
      if (!sendSuccess) {
        throw new Error("Failed to send transaction after multiple attempts");
      }
      
      console.log(`[${new Date().toISOString()}] swapBuy: Transaction ${idx + 1} sent, txId: ${txId}`);
      
      const confirmed = await confirmTransactionWithRetry(txId, blockhash, lastValidBlockHeight, 3);
      if (!confirmed) {
        throw new Error(`Failed to confirm transaction ${txId}`);
      }
      
      console.log(`[${new Date().toISOString()}] swapBuy: Transaction ${idx + 1} confirmed, txId: ${txId}`);
    }
    return { success: true, fee: dynamicMultiplier };
  } catch (error: any) {
    if (retryCount < MAX_RETRIES) {
      console.warn(`[${new Date().toISOString()}] swapBuy: Error on attempt ${retryCount + 1}/${MAX_RETRIES}: ${error.message}`);
      
      // Check if we should switch RPC endpoint
      if (error.message.includes("timeout") || 
          error.message.includes("rate limit") || 
          error.message.includes("429")) {
        switchRpcEndpoint();
      }
      
      // More aggressive backoff strategy
      const backoffDelay = Math.min(500 * Math.pow(1.5, retryCount), 15000);
      await delay(backoffDelay);
      
      // More aggressive fee increase
      const newFeeMultiplier = Math.min(priorityFeeMultiplier * 1.8, MAX_FEE_MULTIPLIER);
      
      return swapBuy(tokenAddress, amount, newFeeMultiplier, retryCount + 1);
    }
    
    console.error(`[${new Date().toISOString()}] swapBuy error after ${MAX_RETRIES} retries:`, error);
    return { success: false, fee: priorityFeeMultiplier };
  }
}

/**
 * swapSell: Swaps the token back to SOL using Raydium.
 * Returns an object { success, fee }.
 */
/**
 * swapSell: Swaps the token back to SOL using Raydium.
 * Returns an object { success, fee }.
 */
export async function swapSell(
  tokenAddress: string,
  tokenAmount: number,
  priorityFeeMultiplier: number = 1,
  retryCount: number = 0,
  slippageOverride: number | null = null
): Promise<{ success: boolean; fee: number }> {
  const MAX_RETRIES = 18; // More retries for sell
  const MAX_FEE_MULTIPLIER = 20; // Higher max fee for sell
  
  try {
    // Get fresh blockhash
    const { blockhash, lastValidBlockHeight } = await getLatestBlockhash();
    
    // Get priority fee data with retry
    let baseFee = 1500000; // Higher default for selling
    try {
      const feeUrl = `${API_URLS.BASE_HOST}${API_URLS.PRIORITY_FEE}`;
      const feeResponse = await axiosRequestWithBackoff({ method: "get", url: feeUrl });
      const feeData = feeResponse.data;
      baseFee = Number(feeData.data.default.h);
    } catch (error) {
      console.warn(`[${new Date().toISOString()}] Fee API error, using default high fee for selling`);
    }
    
    // More aggressive fee increase for sells
    const dynamicMultiplier = priorityFeeMultiplier * (1 + (retryCount * 0.4));
    const computeUnitPriceMicroLamports = String(Math.floor(baseFee * dynamicMultiplier));
    
    // More aggressive slippage for sells
    const effectiveSlippage = slippageOverride !== null ? 
      slippageOverride : 
      (retryCount > 2 ? slippage + (retryCount * 1.5) : slippage);
    
    console.log(`[${new Date().toISOString()}] swapSell using slippage: ${effectiveSlippage}%, fee multiplier: ${dynamicMultiplier.toFixed(2)}`);

    // Verify token balance before proceeding
    const initialBalance = await getTokenBalance(tokenAddress);
    if (initialBalance <= 0) {
      console.log(`[${new Date().toISOString()}] No token balance to sell.`);
      return { success: true, fee: priorityFeeMultiplier }; // Consider as success if nothing to sell
    }
    
    // Adjust amount to ensure it's not more than actual balance
    const amountToSell = Math.min(tokenAmount, initialBalance);
    
    // Get swap quote for token → SOL.
    const sellSwapQuoteUrl = `${API_URLS.SWAP_HOST}/compute/swap-base-in?inputMint=${tokenAddress}&outputMint=${NATIVE_MINT}&amount=${amountToSell}&slippageBps=${effectiveSlippage * 100}&txVersion=V0`;
    const sellSwapQuoteResponse = await axiosRequestWithBackoff({ method: "get", url: sellSwapQuoteUrl });
    const sellSwapResponse = sellSwapQuoteResponse.data;

    // Compute the associated token account (ATA) for the token being sold.
    const ata = await getAssociatedTokenAddress(new PublicKey(tokenAddress), owner.publicKey);

    // Request serialized transaction(s) for the sell swap with higher compute limit
    const sellSwapTxUrl = `${API_URLS.SWAP_HOST}/transaction/swap-base-in`;
    const sellSwapTxResponse = await axiosRequestWithBackoff({
      method: "post",
      url: sellSwapTxUrl,
      data: {
        computeUnitPriceMicroLamports,
        swapResponse: sellSwapResponse,
        txVersion: "V0",
        wallet: owner.publicKey.toBase58(),
        wrapSol: false,
        unwrapSol: true,
        inputAccount: ata.toBase58(),
        computeUnitLimit: 250000, // Even higher compute limit for selling
      },
    });
    const sellSwapTransactions = sellSwapTxResponse.data;

    console.log(`[${new Date().toISOString()}] swapSell parameters:`, {
      computeUnitPriceMicroLamports,
      wallet: owner.publicKey.toBase58(),
      wrapSol: false,
      unwrapSol: true,
      inputAccount: ata.toBase58(),
      multiplier: dynamicMultiplier,
    });
    console.log(`[${new Date().toISOString()}] swapSell transactions received:`, sellSwapTransactions);

    if (!sellSwapTransactions.data || !Array.isArray(sellSwapTransactions.data)) {
      throw new Error("Sell swap response missing transaction data");
    }

    // Deserialize and process each transaction.
    const allTxBuf = sellSwapTransactions.data.map((tx: { transaction: string }) =>
      Buffer.from(tx.transaction, "base64")
    );
    const allTransactions = allTxBuf.map((txBuf: Buffer) =>
      isV0Tx ? VersionedTransaction.deserialize(txBuf) : Transaction.from(txBuf)
    );

    for (const [idx, tx] of allTransactions.entries()) {
      tx.sign([owner]);
      
      // Enhanced transaction options for congested networks
      const txOptions = {
        skipPreflight: true,
        maxRetries: 10,
        preflightCommitment: "processed" as Commitment
      };
      
      // Store initial balance before sending transaction
      const preTxBalance = await getTokenBalance(tokenAddress);
      
      // Send with retries
      let txId = "";
      let sendSuccess = false;
      
      for (let sendAttempt = 0; sendAttempt < 4; sendAttempt++) { // More retries for sell
        try {
          txId = await connection.sendTransaction(tx as VersionedTransaction, txOptions);
          sendSuccess = true;
          break;
        } catch (sendError: any) {
          // Check if the transaction might have gone through despite error
          if (sendAttempt > 0) {
            await delay(3000); // Wait a bit to let the transaction propagate
            const currentBalance = await getTokenBalance(tokenAddress);
            // If balance decreased by at least 50%, consider it a success
            if (currentBalance <= preTxBalance * 0.5) {
              console.log(`[${new Date().toISOString()}] Transaction likely succeeded despite error. Balance reduced from ${preTxBalance} to ${currentBalance}`);
              return { success: true, fee: dynamicMultiplier };
            }
          }
          
          if (sendAttempt < 3) {
            console.warn(`[${new Date().toISOString()}] Sell tx send retry ${sendAttempt+1}/4: ${sendError.message}`);
            await delay(1000);
          } else {
            throw sendError;
          }
        }
      }
      
      if (!sendSuccess) {
        // One final balance check before giving up on this attempt
        const finalCheckBalance = await getTokenBalance(tokenAddress);
        if (finalCheckBalance <= preTxBalance * 0.5) {
          console.log(`[${new Date().toISOString()}] Transaction likely succeeded despite errors. Balance reduced from ${preTxBalance} to ${finalCheckBalance}`);
          return { success: true, fee: dynamicMultiplier };
        }
        throw new Error("Failed to send sell transaction after multiple attempts");
      }
      
      console.log(`[${new Date().toISOString()}] swapSell: Transaction ${idx + 1} sent, txId: ${txId}`);
      
      // Wait a bit for the transaction to propagate before checking
      await delay(4000);
      
      // Check if balance has decreased significantly, which indicates success
      // regardless of confirmation status
      const postTxBalance = await getTokenBalance(tokenAddress);
      if (postTxBalance <= preTxBalance * 0.5) {
        console.log(`[${new Date().toISOString()}] swapSell: Transaction ${idx + 1} likely successful based on balance change. Balance reduced from ${preTxBalance} to ${postTxBalance}`);
        return { success: true, fee: dynamicMultiplier };
      }
      
      // Try the standard confirmation approach as well
      let confirmed = false;
      try {
        confirmed = await confirmTransactionWithRetry(txId, blockhash, lastValidBlockHeight, 5);
      } catch (confirmError: unknown) {
        // Type-safe error handling
        const errorMessage = confirmError instanceof Error 
          ? confirmError.message 
          : String(confirmError);
        console.warn(`[${new Date().toISOString()}] Confirmation error for ${txId}: ${errorMessage}`);
        
        // Check balance again to see if the transaction went through despite confirmation errors
        const postConfirmBalance = await getTokenBalance(tokenAddress);
        if (postConfirmBalance <= preTxBalance * 0.5) {
          console.log(`[${new Date().toISOString()}] Transaction successful despite confirmation error. Balance reduced from ${preTxBalance} to ${postConfirmBalance}`);
          return { success: true, fee: dynamicMultiplier };
        }
      }
      
      if (!confirmed) {
        // One final balance check before failing
        const finalBalance = await getTokenBalance(tokenAddress);
        if (finalBalance <= preTxBalance * 0.5) {
          console.log(`[${new Date().toISOString()}] Transaction succeeded but confirmation failed. Balance reduced from ${preTxBalance} to ${finalBalance}`);
          return { success: true, fee: dynamicMultiplier };
        }
        throw new Error(`Failed to confirm sell transaction ${txId}`);
      }
      
      console.log(`[${new Date().toISOString()}] swapSell: Transaction ${idx + 1} confirmed, txId: ${txId}`);
    }
    
    // Verify if sell was successful by checking balance
    const finalBalance = await getTokenBalance(tokenAddress);
    if (finalBalance > 0 && finalBalance >= amountToSell * 0.95) { // Still has most of balance
      throw new Error("Sell appeared to succeed but token balance remains high");
    }
    
    return { success: true, fee: dynamicMultiplier };
  } catch (error: any) {
    // Check balance one more time before retrying - maybe transaction went through
    try {
      const currentBalance = await getTokenBalance(tokenAddress);
      if (currentBalance <= 0 || currentBalance < tokenAmount * 0.1) {
        console.log(`[${new Date().toISOString()}] swapSell: Error occurred but token balance is very low (${currentBalance}). Considering sell successful.`);
        return { success: true, fee: priorityFeeMultiplier };
      }
    } catch (balanceError: unknown) {
      // Type-safe error handling
      const errorMessage = balanceError instanceof Error 
        ? balanceError.message 
        : String(balanceError);
      console.warn(`[${new Date().toISOString()}] Failed to check balance after error: ${errorMessage}`);
    }
    
    // More aggressive retry logic for sells to ensure we don't get stuck with tokens
    if (retryCount < MAX_RETRIES) {
      console.warn(`[${new Date().toISOString()}] swapSell: Error on attempt ${retryCount + 1}/${MAX_RETRIES}: ${error.message}`);
      
      // Check if we should switch RPC endpoint
      if (error.message.includes("timeout") || 
          error.message.includes("rate limit") || 
          error.message.includes("429")) {
        switchRpcEndpoint();
      }
      
      // If this is an expired transaction error, try with higher fee immediately
      const isExpiredTx = error?.message?.includes("TransactionExpiredBlockheightExceededError") ||
                         error?.message?.includes("blockhash not found");
      
      // Exponential backoff delay, but shorter for expired transaction errors
      const backoffDelay = isExpiredTx 
        ? 500 
        : Math.min(1000 * Math.pow(1.5, retryCount), 10000);
      
      await delay(backoffDelay);
      
      // More aggressive fee increase for sells, especially on expired transaction errors
      let newFeeMultiplier = isExpiredTx 
        ? Math.min(priorityFeeMultiplier * 2.5, MAX_FEE_MULTIPLIER)
        : Math.min(priorityFeeMultiplier * 1.8, MAX_FEE_MULTIPLIER);
      
      // If we're getting close to the max retries, be more aggressive with fees
      if (retryCount > MAX_RETRIES * 0.7) {
        newFeeMultiplier = Math.min(newFeeMultiplier * 1.5, MAX_FEE_MULTIPLIER);
      }
      
      // If the token amount is high and we're struggling, try with a slightly smaller amount
      let adjustedAmount = tokenAmount;
      if (retryCount > MAX_RETRIES * 0.3 && tokenAmount > 2) {
        adjustedAmount = tokenAmount * 0.95; // Try with 95% of the balance
        console.log(`[${new Date().toISOString()}] swapSell: Trying with slightly reduced amount: ${adjustedAmount}`);
      }
      
      // Try chunking transactions after multiple failures
      if (retryCount > MAX_RETRIES * 0.7 && tokenAmount > 10) {
        console.log(`[${new Date().toISOString()}] swapSell: Attempting to sell in chunks`);
        return sellInChunks(tokenAddress, tokenAmount, newFeeMultiplier);
      }
      
      return swapSell(tokenAddress, adjustedAmount, newFeeMultiplier, retryCount + 1);
    }
    
    console.error(`[${new Date().toISOString()}] swapSell error after ${MAX_RETRIES} retries:`, error);
    
    // Last resort emergency sell attempt with extreme parameters
    return emergencySellAttempt(tokenAddress, tokenAmount);
  }
}

/**
 * Helper function to sell tokens in chunks
 */
async function sellInChunks(
  tokenAddress: string,
  totalAmount: number,
  feeMultiplier: number
): Promise<{ success: boolean; fee: number }> {
  console.log(`[${new Date().toISOString()}] Attempting to sell in smaller chunks with high fees`);
  
  // Get current balance
  const currentBalance = await getTokenBalance(tokenAddress);
  if (currentBalance <= 0) return { success: true, fee: feeMultiplier };
  
  // Define chunks (start small then increase)
  const chunks = [
    currentBalance * 0.2,  // 20%
    currentBalance * 0.3,  // 30%
    currentBalance * 0.5   // 50%
  ];
  
  let successCount = 0;
  let highestFeeUsed = feeMultiplier;
  
  // Try selling each chunk with very high slippage and fees
  for (let i = 0; i < chunks.length; i++) {
    const chunkAmount = chunks[i];
    const remainingBalance = await getTokenBalance(tokenAddress);
    
    if (remainingBalance <= 0) break;
    
    const actualChunkAmount = Math.min(chunkAmount, remainingBalance);
    console.log(`[${new Date().toISOString()}] Selling chunk ${i+1}: ${actualChunkAmount} tokens (${((actualChunkAmount/totalAmount)*100).toFixed(1)}% of total)`);
    
    // Use increasingly aggressive parameters
    const chunkFeeMultiplier = Math.min(feeMultiplier * (1.5 + i * 0.5), 30);
    const chunkSlippage = Math.min(10 + i * 5, 30); // 10%, 15%, 20%
    
    try {
      const result = await swapSell(tokenAddress, actualChunkAmount, chunkFeeMultiplier, 0, chunkSlippage);
      if (result.success) {
        successCount++;
        highestFeeUsed = Math.max(highestFeeUsed, result.fee);
        await delay(5000); // Wait before next chunk
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error selling chunk ${i+1}:`, error);
      continue;
    }
  }
  
  // Final check
  const finalBalance = await getTokenBalance(tokenAddress);
  const success = finalBalance < totalAmount * 0.1; // Consider success if we sold 90%+
  
  return { 
    success, 
    fee: highestFeeUsed 
  };
}

/**
 * Last resort emergency sell attempt with extreme parameters
 */
async function emergencySellAttempt(
  tokenAddress: string,
  tokenAmount: number
): Promise<{ success: boolean; fee: number }> {
  console.log(`[${new Date().toISOString()}] EMERGENCY SELL ATTEMPT with extreme parameters`);
  
  try {
    // Check current balance
    const currentBalance = await getTokenBalance(tokenAddress);
    if (currentBalance <= 0) return { success: true, fee: 1 };
    
    // Try multiple approaches in parallel
    const emergencyFee = 30; // Extreme fee
    const emergencySlippage = 50; // Extreme slippage (50%)
    
    // Try different amounts in parallel
    const attempts = [
      // Try full amount with extreme parameters
      swapSell(tokenAddress, currentBalance, emergencyFee, 0, emergencySlippage),
      
      // Try half amount
      swapSell(tokenAddress, currentBalance * 0.5, emergencyFee * 1.2, 0, emergencySlippage + 10),
      
      // Try quarter amount
      swapSell(tokenAddress, currentBalance * 0.25, emergencyFee * 1.5, 0, emergencySlippage + 20)
    ];
    
    // Wait for any success
    const results = await Promise.all(
      attempts.map(p => p.catch(e => ({ success: false })))
    );
    const anySuccess = results.some(
      (r: any) => r.status === 'fulfilled' && r.value.success
    );
    
    // Check if we still have a balance
    const remainingBalance = await getTokenBalance(tokenAddress);
    const significantlySold = remainingBalance < currentBalance * 0.5;
    
    return { 
      success: anySuccess || significantlySold, 
      fee: emergencyFee 
    };
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Emergency sell failed:`, error);
    return { success: false, fee: 30 };
  }
 }
 
 /**
 * getTokenPrice: Gets current price of token in SOL
 */
 export async function getTokenPrice(tokenAddress: string): Promise<number | null> {
  try {
    const { LAMPORTS_PER_SOL } = require("@solana/web3.js");
    // Use Raydium API to get the current price
    const priceUrl = `${API_URLS.SWAP_HOST}/compute/swap-base-out?inputMint=${tokenAddress}&outputMint=${NATIVE_MINT}&amount=1000000&slippageBps=10`;
    
    // Retry up to 3 times with backoff
    let response;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        response = await axiosRequestWithBackoff({ method: "get", url: priceUrl });
        break;
      } catch (error) {
        if (attempt === 2) throw error;
        await delay(1000 * Math.pow(2, attempt));
      }
    }
    
    if (response?.data && response.data.quotedAmountIn && response.data.quotedAmountOut) {
      // Calculate price based on the swap quote
      const inputAmount = Number(response.data.quotedAmountIn);
      const outputAmount = Number(response.data.quotedAmountOut) / LAMPORTS_PER_SOL; // Convert lamports to SOL
      
      if (inputAmount > 0) {
        return outputAmount / (inputAmount / 1000000); // Price per token in SOL
      }
    }
    
    // Try alternative price calculation method if the first fails
    try {
      // Get a small swap quote in the opposite direction
      const altPriceUrl = `${API_URLS.SWAP_HOST}/compute/swap-base-in?inputMint=${NATIVE_MINT}&outputMint=${tokenAddress}&amount=${0.01 * LAMPORTS_PER_SOL}&slippageBps=100`;
      const altResponse = await axiosRequestWithBackoff({ method: "get", url: altPriceUrl });
      
      if (altResponse?.data && altResponse.data.quotedAmountIn && altResponse.data.quotedAmountOut) {
        const solAmount = Number(altResponse.data.quotedAmountIn) / LAMPORTS_PER_SOL;
        const tokenAmount = Number(altResponse.data.quotedAmountOut);
        
        if (tokenAmount > 0) {
          // Calculate SOL per token and then invert
          const tokenPrice = solAmount / tokenAmount;
          console.log(`[${new Date().toISOString()}] Alternative price calculation for ${tokenAddress}: ${tokenPrice} SOL/token`);
          return tokenPrice;
        }
      }
    } catch (altError) {
      console.warn(`[${new Date().toISOString()}] Alternative price calculation failed:`, altError);
    }
    
    console.warn(`[${new Date().toISOString()}] Could not determine token price for ${tokenAddress}`);
    return null;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error getting token price for ${tokenAddress}:`, error);
    return null;
  }
 }
 
 /**
 * getTokenBalance: Retrieves the token balance for the associated token account.
 * Now with improved error handling and retry logic.
 */
 export async function getTokenBalance(tokenAddress: string): Promise<number> {
  const MAX_RETRIES = 3;
  
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const ata = await getAssociatedTokenAddress(new PublicKey(tokenAddress), owner.publicKey);
      const balanceInfo = await connection.getTokenAccountBalance(ata);
      return parseFloat(balanceInfo.value.amount);
    } catch (error: any) {
      // If this is the last attempt, just return 0
      if (attempt === MAX_RETRIES - 1) {
        console.error(`[${new Date().toISOString()}] getTokenBalance failed after ${MAX_RETRIES} attempts for ${tokenAddress}:`, error);
        return 0;
      }
      
      // Check if it's a "Token account not found" error which means balance is 0
      if (error.toString().includes("account not found") || 
          error.toString().includes("not exist") || 
          error.toString().includes("invalid account")) {
        return 0;
      }
      
      console.warn(`[${new Date().toISOString()}] getTokenBalance retry ${attempt + 1}/${MAX_RETRIES}:`, error);
      await delay(1000);
      
      // Try switching RPC if needed
      if (error.toString().includes("timeout") || 
          error.toString().includes("rate limit") || 
          error.toString().includes("429")) {
        switchRpcEndpoint();
      }
    }
  }
  
  return 0;
 }
 
 /**
 * Track solana account balance to monitor overall profits/losses
 */
 export async function getSolBalance(): Promise<number> {
  try {
    const balance = await connection.getBalance(owner.publicKey);
    return balance / 1_000_000_000; // Convert lamports to SOL
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error getting SOL balance:`, error);
    return 0;
  }
 }
 
 /**
 * Monitor transaction fees over time to optimize strategies
 */
 export async function getRecentFeesSpent(
  lookbackSlots: number = 100
 ): Promise<number> {
  try {
    // Get recent signatures for the wallet
    const signatures = await connection.getSignaturesForAddress(
      owner.publicKey, 
      { limit: 10 }
    );
    
    if (!signatures.length) return 0;
    
    let totalFees = 0;
    for (const sigInfo of signatures) {
      const tx = await connection.getTransaction(sigInfo.signature, {
        maxSupportedTransactionVersion: 0
      });
      
      if (tx && tx.meta) {
        totalFees += tx.meta.fee;
      }
    }
    
    return totalFees / 1_000_000_000; // Return in SOL
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error calculating recent fees:`, error);
    return 0;
  }
 }
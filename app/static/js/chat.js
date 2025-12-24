document.addEventListener('DOMContentLoaded', () => {
    const chatMessages = document.getElementById('chat-messages');
    const messageInput = document.getElementById('message-input');
    const sendButton = document.getElementById('send-button');
    const connectionStatus = document.getElementById('connection-status');

    // Determine WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/ws/chat`;
    
    // Create WebSocket connection
    const socket = new WebSocket(wsUrl);

    // Connection opened
    socket.addEventListener('open', (event) => {
        console.log('WebSocket connection established');
        connectionStatus.textContent = 'Connected';
        connectionStatus.style.backgroundColor = '#10b981';
        sendButton.disabled = false;
    });

    // Listen for messages
    socket.addEventListener('message', (event) => {
        // Remove typing indicator if present
        const typingIndicator = document.querySelector('.typing-indicator');
        if (typingIndicator) {
            typingIndicator.remove();
        }
        
        addMessage('assistant', event.data);
        scrollToBottom();
    });

    // Handle errors
    socket.addEventListener('error', (error) => {
        console.error('WebSocket error:', error);
        connectionStatus.textContent = 'Connection error';
        connectionStatus.style.backgroundColor = '#ef4444';
    });

    // Connection closed
    socket.addEventListener('close', (event) => {
        console.log('WebSocket connection closed:', event);
        connectionStatus.textContent = 'Disconnected';
        connectionStatus.style.backgroundColor = '#ef4444';
        sendButton.disabled = true;
        
        // Show reconnection message
        if (!event.wasClean) {
            addMessage('system', 'Connection lost. Trying to reconnect...');
            // Try to reconnect after 5 seconds
            setTimeout(() => window.location.reload(), 5000);
        }
    });

    // Send message when clicking the send button
    sendButton.addEventListener('click', sendMessage);

    // Send message when pressing Enter (but allow Shift+Enter for new lines)
    messageInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    // Auto-resize textarea as user types
    messageInput.addEventListener('input', () => {
        messageInput.style.height = 'auto';
        messageInput.style.height = (messageInput.scrollHeight) + 'px';
    });

    function sendMessage() {
        const message = messageInput.value.trim();
        if (!message) return;

        // Add user message to chat
        addMessage('user', message);
        
        // Show typing indicator
        const typingIndicator = document.createElement('div');
        typingIndicator.className = 'message assistant typing-indicator';
        typingIndicator.innerHTML = `
            <div class="typing">
                <span class="typing-dot"></span>
                <span class="typing-dot"></span>
                <span class="typing-dot"></span>
            </div>
        `;
        chatMessages.appendChild(typingIndicator);
        
        // Send message through WebSocket
        socket.send(message);
        
        // Clear input and reset height
        messageInput.value = '';
        messageInput.style.height = 'auto';
        
        // Scroll to bottom
        scrollToBottom();
    }

    function addMessage(role, content) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;
        messageDiv.innerHTML = `<div class="message-content">${formatMessage(content)}</div>`;
        chatMessages.appendChild(messageDiv);
    }

    function formatMessage(text) {
        // Simple URL detection and linking
        return text.replace(
            /(https?:\/\/[^\s]+)/g, 
            url => `<a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a>`
        );
    }

    function scrollToBottom() {
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    // Initial scroll to bottom
    scrollToBottom();
});

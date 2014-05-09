
std::string int2str(int &i) {
	std::string s;
	std::stringstream ss(s);
	ss << i;
	return ss.str();
}

